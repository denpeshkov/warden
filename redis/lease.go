package redis

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DefaultLeaseTTL is the default TTL for a lease.
	DefaultLeaseTTL = 30 * time.Second
	// RedisLeasePrefix is a prefix used for Redis keys.
	RedisLeasePrefix = "warden:lease:"
)

// ErrConflict is returned when the lease is held by another owner or doesn't exist.
var ErrConflict = errors.New("lease is held by another owner")

var (
	//go:embed lua/lock.lua
	luaLockScript string
	//go:embed lua/unlock.lua
	luaUnlockScript string
	//go:embed lua/renew.lua
	luaRenewScript string

	lockScript   = redis.NewScript(luaLockScript)
	unlockScript = redis.NewScript(luaUnlockScript)
	renewScript  = redis.NewScript(luaRenewScript)
)

// Manager manages distributed leases.
type Manager struct {
	rds    redis.UniversalClient
	nodeID string
}

// NewManager creates a new [Manager] with the given Redis client.
func NewManager(rds redis.UniversalClient) *Manager {
	nodeID, _ := os.Hostname()
	nodeID = fmt.Sprintf("%s-%d", nodeID, os.Getpid())
	return &Manager{
		rds:    rds,
		nodeID: nodeID,
	}
}

// Lease creates a new [Lease] for the given key with the default configuration.
// The lease is not acquired until [Lock] or [TryLock] is called on it.
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
// A Lease is safe for concurrent use by multiple goroutines.
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

	pubsub := l.m.rds.Subscribe(ctx, RedisLeasePrefix+"notify:"+l.Key)
	defer func() { _ = pubsub.Close() }()

	ch := pubsub.Channel()
	for {
		timeout := max(remaining, 10*time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeout):
			// Timeout expired - retry in case lock TTL elapsed.
		case <-ch:
			// Got release notification.
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
	res, err := unlockScript.Run(
		ctx,
		l.m.rds,
		[]string{RedisLeasePrefix + l.Key},
		l.FencingToken,
		RedisLeasePrefix+"notify:"+l.Key,
	).Result()
	if err != nil {
		return err
	}

	released, ok := res.(int64)
	if !ok {
		return fmt.Errorf("unexpected script result type: %T", res)
	}
	if released != 1 {
		return ErrConflict
	}
	return nil
}

// Renew renews the lease by extending the TTL.
// Returns [ErrConflict] if the lease is not held by this fencing token.
func (l *Lease) Renew(ctx context.Context) error {
	res, err := renewScript.Run(
		ctx,
		l.m.rds,
		[]string{RedisLeasePrefix + l.Key},
		l.FencingToken,
		int64(l.TTL/time.Millisecond),
	).Result()
	if err != nil {
		return err
	}

	extended, ok := res.(int64)
	if !ok {
		return fmt.Errorf("unexpected script result type: %T", res)
	}
	if extended != 1 {
		return ErrConflict
	}
	return nil
}

// Expiry returns the remaining TTL of the lease.
func (l *Lease) Expiry(ctx context.Context) (time.Duration, error) {
	return l.m.rds.PTTL(ctx, RedisLeasePrefix+l.Key).Result()
}

// tryLock attempts to acquire the lease without blocking.
// Returns (0, nil) on success, or (remaining TTL, ErrConflict) if the lease is held by another owner.
func (l *Lease) tryLock(ctx context.Context) (time.Duration, error) {
	result, err := lockScript.Run(
		ctx,
		l.m.rds,
		[]string{RedisLeasePrefix + l.Key},
		l.FencingToken,
		int64(l.TTL/time.Millisecond),
	).Result()
	if err != nil {
		return 0, err
	}

	res, ok := result.([]any)
	if !ok || len(res) < 2 {
		return 0, fmt.Errorf("unexpected script result type: %T", result)
	}

	acquired, ok := res[0].(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected acquired value type: %T", res[0])
	}
	if acquired == 1 {
		return 0, nil
	}
	remaining, ok := res[1].(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected remaining value type: %T", res[1])
	}
	return time.Duration(remaining) * time.Millisecond, ErrConflict
}
