package apikey

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type APIKey struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id"`
	Prefix      string     `json:"prefix"`
	Label       string     `json:"label"`
	Permissions []string   `json:"permissions"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

type APIKeyWithRaw struct {
	APIKey
	RawKey string `json:"raw_key"`
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// Create generates a new API key. The raw key is returned only once.
func (s *Store) Create(ctx context.Context, tenantID, label string) (*APIKeyWithRaw, error) {
	rawKey, prefix, hash, err := generateKey()
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	k := &APIKeyWithRaw{RawKey: rawKey}
	err = s.db.QueryRowContext(ctx,
		`INSERT INTO api_keys (tenant_id, key_hash, prefix, label)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, tenant_id, prefix, label, permissions, created_at`,
		tenantID, hash, prefix, label,
	).Scan(&k.ID, &k.TenantID, &k.Prefix, &k.Label, pq.Array(&k.Permissions), &k.CreatedAt)
	if err != nil {
		return nil, err
	}
	return k, nil
}

// Lookup finds an active API key by prefix, returns the hash for verification.
func (s *Store) Lookup(ctx context.Context, prefix string) (*APIKey, string, error) {
	k := &APIKey{}
	var hash string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, prefix, label, permissions, key_hash, created_at
		 FROM api_keys
		 WHERE prefix = $1 AND revoked_at IS NULL`,
		prefix,
	).Scan(&k.ID, &k.TenantID, &k.Prefix, &k.Label, pq.Array(&k.Permissions), &hash, &k.CreatedAt)
	if err != nil {
		return nil, "", err
	}
	return k, hash, nil
}

func (s *Store) ListByTenant(ctx context.Context, tenantID string) ([]APIKey, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, prefix, label, permissions, revoked_at, created_at
		 FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.TenantID, &k.Prefix, &k.Label, pq.Array(&k.Permissions), &k.RevokedAt, &k.CreatedAt); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (s *Store) Revoke(ctx context.Context, tenantID, keyID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE api_keys SET revoked_at = now()
		 WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL`,
		keyID, tenantID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// HashKey produces the SHA-256 hash used for key verification.
func HashKey(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

func generateKey() (raw, prefix, hash string, err error) {
	b := make([]byte, 32)
	if _, err = rand.Read(b); err != nil {
		return "", "", "", err
	}
	raw = "elk_" + hex.EncodeToString(b)
	prefix = raw[:12]
	hash = HashKey(raw)
	return raw, prefix, hash, nil
}
