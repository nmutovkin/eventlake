package tenant

import (
	"context"
	"database/sql"
	"time"
)

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Plan      string    `json:"plan"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, name string) (*Tenant, error) {
	t := &Tenant{}
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO tenants (name) VALUES ($1)
		 RETURNING id, name, plan, created_at, updated_at`,
		name,
	).Scan(&t.ID, &t.Name, &t.Plan, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Store) GetByID(ctx context.Context, id string) (*Tenant, error) {
	t := &Tenant{}
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, plan, created_at, updated_at FROM tenants WHERE id = $1`,
		id,
	).Scan(&t.ID, &t.Name, &t.Plan, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Store) List(ctx context.Context) ([]Tenant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, plan, created_at, updated_at FROM tenants ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tenants []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.Plan, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, rows.Err()
}
