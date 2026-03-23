package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const StreamName = "eventlake:events"

type EnvelopedEvent struct {
	TenantID   string `json:"tenant_id"`
	ReceivedAt string `json:"received_at"`
	Event
}

type Publisher struct {
	rdb *redis.Client
}

func NewPublisher(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb}
}

// Publish sends a batch of validated events to the Redis stream.
func (p *Publisher) Publish(ctx context.Context, tenantID string, events []Event) error {
	pipe := p.rdb.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, e := range events {
		env := EnvelopedEvent{
			TenantID:   tenantID,
			ReceivedAt: now,
			Event:      e,
		}
		data, err := json.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: StreamName,
			Values: map[string]any{"data": string(data)},
		})
	}

	_, err := pipe.Exec(ctx)
	return err
}
