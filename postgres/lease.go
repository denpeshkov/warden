// Package postgres provides a lease backed by PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultLeaseTTL is the default TTL for a lease.
const DefaultLeaseTTL = 30 * time.Second

// ErrConflict is returned when the lease is held by another owner or doesn't exist.
var ErrConflict = errors.New("lease is held by another owner")

// CreateLockTableSQL is the SQL statement to create the lease table.
const CreateLockTableSQL = `
create table if not exists lease (
    key text,
    fencing_token text not null,
    acquired_at timestamptz not null,
    expires_at timestamptz not null,

    constraint pk_lease primary key (key)
);
create index if not exists index_lease_on_expires_at on lease (expires_at);`

// Manager manages distributed leases using PostgreSQL.
// It provides methods to create and manage leases across multiple nodes.
type Manager struct {
	pool   *pgxpool.Pool
	nodeID string
}

// NewManager creates a new [Manager] with the given PostgreSQL connection pool.
// The node ID is automatically generated from the hostname and process ID.
// It also starts a background goroutine to clean up expired locks.
func NewManager(pool *pgxpool.Pool) *Manager {
	nodeID, _ := os.Hostname()
	nodeID = fmt.Sprintf("%s-%d", nodeID, os.Getpid())

	manager := &Manager{
		pool:   pool,
		nodeID: nodeID,
	}
	return manager
}

// Lease creates a new [Lease] for the given key with the default configuration.
// The lease is not acquired until [Lease.Lock] or [Lease.TryLock] is called on it.
func (m *Manager) Lease(key string) *Lease {
	return &Lease{
		Key:          key,
		FencingToken: m.fencingToken(),
		TTL:          DefaultLeaseTTL,
		m:            m,
	}
}

func (m *Manager) fencingToken() string {
	return fmt.Sprintf("%s-%d-%d", m.nodeID, time.Now().UnixNano(), rand.Int64()) //nolint:gosec
}

// Lease represents a distributed lease on a key.
type Lease struct {
	// Key is the lease identifier.
	Key string
	// FencingToken is a unique token used to prevent conflicts when releasing or renewing the lease.
	FencingToken string
	// TTL is the time-to-live duration for the lease.
	TTL time.Duration
	m   *Manager
}

// Lock acquires the lease, blocking until the lock is available or the context is canceled.
func (l *Lease) Lock(ctx context.Context) error {
	remaining, err := l.tryLock(ctx)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrConflict) {
		return err
	}

	conn, err := l.m.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "listen "+pgx.Identifier{l.Key}.Sanitize()); err != nil {
		return fmt.Errorf("listen %q: %w", l.Key, err)
	}
	for {
		if err := func() error {
			timeout := max(remaining, 10*time.Millisecond)
			wctx, stop := context.WithTimeout(ctx, timeout)
			defer stop()
			if _, err := conn.Conn().WaitForNotification(wctx); err != nil && wctx.Err() == nil {
				return err
			}
			return ctx.Err()
		}(); err != nil {
			return err
		}
		remaining, err = l.tryLock(ctx)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
	}
}

// TryLock attempts to acquire the lease without blocking.
// Returns [ErrConflict] if the lease is held by another owner.
func (l *Lease) TryLock(ctx context.Context) error {
	_, err := l.tryLock(ctx)
	return err
}

// Unlock releases the lease.
// Returns [ErrConflict] if the lease is not held by this fencing token.
func (l *Lease) Unlock(ctx context.Context) error {
	ct, err := l.m.pool.Exec(ctx, `delete from lease where key = $1 and fencing_token = $2`, l.Key, l.FencingToken)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

// Renew renews the lease by extending the TTL.
// Returns [ErrConflict] if the lease is not held by this fencing token.
func (l *Lease) Renew(ctx context.Context) error {
	ct, err := l.m.pool.Exec(ctx, `
		update lease set expires_at = $1
		where key = $2 and fencing_token = $3 and expires_at > now()
	`, time.Now().Add(l.TTL), l.Key, l.FencingToken)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

// Expiry returns the remaining TTL of the lease.
func (l *Lease) Expiry(ctx context.Context) (time.Duration, error) {
	var expiresAt time.Time
	if err := l.m.pool.QueryRow(ctx, `select expires_at from lease where key = $1`, l.Key).Scan(&expiresAt); err != nil {
		return 0, err
	}
	return time.Until(expiresAt), nil
}

func (l *Lease) tryLock(ctx context.Context) (time.Duration, error) {
	var expiresAt time.Time
	err := l.m.pool.QueryRow(ctx, `
		insert into lease (key, fencing_token, acquired_at, expires_at)
		values ($1, $2, $3, $4)
		on conflict (key) do update set
		fencing_token = excluded.fencing_token,
			acquired_at = excluded.acquired_at,
			expires_at = excluded.expires_at
		where lease.expires_at < now()
		returning expires_at`,
		l.Key, l.FencingToken, time.Now(), time.Now().Add(l.TTL)).Scan(&expiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrConflict
		}
		return 0, err
	}
	return time.Until(expiresAt), nil
}
