package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nmutovkin/eventlake/internal/ingest"
)

const (
	batchSize    = 500
	batchTimeout = 1 * time.Second
	group        = "writers"
	consumer     = "writer-1"
)

type Writer struct {
	db  *sql.DB
	rdb *redis.Client

	// track which tenant partitions we've already ensured
	knownPartitions map[string]bool
}

func NewWriter(db *sql.DB, rdb *redis.Client) *Writer {
	return &Writer{
		db:              db,
		rdb:             rdb,
		knownPartitions: make(map[string]bool),
	}
}

func (w *Writer) Run(ctx context.Context) error {
	// Create consumer group (ignore error if already exists)
	w.rdb.XGroupCreateMkStream(ctx, ingest.StreamName, group, "0").Err()

	log.Println("write worker started")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgs, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{ingest.StreamName, ">"},
			Count:    int64(batchSize),
			Block:    batchTimeout,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Printf("read stream error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, stream := range msgs {
			if len(stream.Messages) == 0 {
				continue
			}
			if err := w.processBatch(ctx, stream.Messages); err != nil {
				log.Printf("process batch error: %v", err)
			}
		}
	}
}

func (w *Writer) processBatch(ctx context.Context, messages []redis.XMessage) error {
	events := make([]ingest.EnvelopedEvent, 0, len(messages))
	ids := make([]string, 0, len(messages))

	for _, msg := range messages {
		data, ok := msg.Values["data"].(string)
		if !ok {
			log.Printf("skip message %s: missing data field", msg.ID)
			ids = append(ids, msg.ID)
			continue
		}
		var env ingest.EnvelopedEvent
		if err := json.Unmarshal([]byte(data), &env); err != nil {
			log.Printf("skip message %s: unmarshal error: %v", msg.ID, err)
			ids = append(ids, msg.ID)
			continue
		}
		events = append(events, env)
		ids = append(ids, msg.ID)
	}

	if len(events) > 0 {
		if err := w.batchInsert(ctx, events); err != nil {
			return fmt.Errorf("batch insert: %w", err)
		}
	}

	// Acknowledge processed messages
	if len(ids) > 0 {
		w.rdb.XAck(ctx, ingest.StreamName, group, ids...)
	}

	log.Printf("processed %d events", len(events))
	return nil
}

func (w *Writer) batchInsert(ctx context.Context, events []ingest.EnvelopedEvent) error {
	// Ensure partitions exist for all tenants in this batch
	for _, e := range events {
		if !w.knownPartitions[e.TenantID] {
			if err := EnsurePartition(ctx, w.db, e.TenantID); err != nil {
				return err
			}
			w.knownPartitions[e.TenantID] = true
		}
	}

	// Build multi-row INSERT
	var b strings.Builder
	b.WriteString(`INSERT INTO events (tenant_id, event_type, timestamp, received_at, idempotency_key, user_id, session_id, properties) VALUES `)

	args := make([]any, 0, len(events)*8)
	for i, e := range events {
		if i > 0 {
			b.WriteString(",")
		}
		offset := i * 8
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			offset+1, offset+2, offset+3, offset+4,
			offset+5, offset+6, offset+7, offset+8,
		)

		props, _ := json.Marshal(e.Properties)

		receivedAt, _ := time.Parse(time.RFC3339Nano, e.ReceivedAt)

		args = append(args,
			e.TenantID,
			e.EventType,
			e.Timestamp,
			receivedAt,
			nilIfEmpty(e.IdempotencyKey),
			nilIfEmpty(e.UserID),
			nilIfEmpty(e.SessionID),
			string(props),
		)
	}

	_, err := w.db.ExecContext(ctx, b.String(), args...)
	return err
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
