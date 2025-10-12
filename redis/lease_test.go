package redis

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/redis/go-redis/v9"
)

var manager *Manager

func TestMain(m *testing.M) {
	rds := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_URL")})
	manager = NewManager(rds)

	code := m.Run()

	if err := rds.FlushDB(context.Background()).Err(); err != nil {
		log.Fatal(err)
	}
	if err := rds.Close(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestLease_ContextTimeout(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// First lease acquires the lock.
	{
		lease := manager.Lease(key)

		err := lease.Lock(t.Context())
		be.Err(t, err, nil)
		t.Cleanup(func() {
			be.Err(t, lease.Unlock(context.Background()), nil)
		})
	}

	// Second lease tries to acquire with timeout.
	{
		lease := manager.Lease(key)
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		t.Cleanup(cancel)

		err := lease.Lock(ctx)
		be.Err(t, err, context.DeadlineExceeded)
	}
}

func TestLease_ContextCancel(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// First lease acquires the lock.
	{
		lease := manager.Lease(key)

		err := lease.Lock(t.Context())
		be.Err(t, err, nil)
		t.Cleanup(func() {
			be.Err(t, lease.Unlock(context.Background()), nil)
		})
	}

	// Second lease tries to acquire with cancellable context.
	{
		lease := manager.Lease(key)
		ctx, cancel := context.WithCancel(t.Context())

		errCh := make(chan error, 1)
		go func() {
			errCh <- lease.Lock(ctx)
		}()

		// Give time for Lock to enter waiting state.
		time.Sleep(50 * time.Millisecond)

		// Cancel the context.
		cancel()

		// Wait for Lock to return with a reasonable timeout.
		select {
		case err := <-errCh:
			be.Err(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Error("Lock did not return after context cancellation")
		}
	}
}

func TestLease_Lock_Concurrent(t *testing.T) {
	t.Parallel()

	key := t.Name()
	lease := manager.Lease(key)

	var successCount atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
			defer cancel()

			if err := lease.TryLock(ctx); err != nil {
				return
			}
			successCount.Add(1)
			time.Sleep(100 * time.Millisecond) // Simulate work.
			uerr := lease.Unlock(ctx)
			be.Err(t, uerr, context.DeadlineExceeded, nil)
		})
	}
	wg.Wait()

	be.True(t, successCount.Load() <= 10)
}

func TestLease_Expiration(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// Acquire lease with short TTL.
	lease1 := manager.Lease(key)
	lease1.TTL = 100 * time.Millisecond
	err := lease1.Lock(t.Context())
	be.Err(t, err, nil)

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Should be able to acquire again.
	lease2 := manager.Lease(key)
	err = lease2.Lock(t.Context())
	be.Err(t, err, nil)
	t.Cleanup(func() {
		be.Err(t, lease2.Unlock(context.Background()), nil)
	})

	be.True(t, lease1.FencingToken != lease2.FencingToken)
}

func TestLease_Renew_Success(t *testing.T) {
	t.Parallel()

	key := t.Name()

	lease := manager.Lease(key)
	lease.TTL = 1 * time.Second

	err := lease.Lock(t.Context())
	be.Err(t, err, nil)
	t.Cleanup(func() {
		err := lease.Unlock(context.Background())
		be.Err(t, err, nil)
	})

	// Renew should succeed.
	err = lease.Renew(t.Context())
	be.Err(t, err, nil)

	// Verify lock is still held by checking TTL was extended.
	remaining, err := lease.Expiry(t.Context())
	be.Err(t, err, nil)
	be.True(t, remaining >= 500*time.Millisecond)
}

func TestLease_Renew_NotOwner(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// First lease acquires the lock.
	{
		lease := manager.Lease(key)
		err := lease.Lock(t.Context())
		be.Err(t, err, nil)
		t.Cleanup(func() {
			err := lease.Unlock(context.Background())
			be.Err(t, err, nil)
		})
	}

	// Second lease tries to renew - should fail.
	{
		lease := manager.Lease(key)
		err := lease.Renew(t.Context())
		be.Err(t, err, ErrConflict)
	}
}

func TestLease_Renew_NoLock(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// Try to renew a lock that doesn't exist.
	lease := manager.Lease(key)
	err := lease.Renew(t.Context())
	be.Err(t, err, ErrConflict)
}

func TestLease_Renew_AfterExpiration(t *testing.T) {
	t.Parallel()

	key := t.Name()

	lease := manager.Lease(key)
	lease.TTL = 100 * time.Millisecond

	err := lease.Lock(t.Context())
	be.Err(t, err, nil)

	// Wait for expiration.
	time.Sleep(200 * time.Millisecond)

	// Renew should fail - lock expired.
	err = lease.Renew(t.Context())
	be.Err(t, err, ErrConflict)
}

func TestLease_Lock_WaitsForNotification(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// First lease acquires the lock.
	lease1 := manager.Lease(key)
	err := lease1.Lock(t.Context())
	be.Err(t, err, nil)

	// Second lease starts blocking.
	lease2 := manager.Lease(key)

	errCh := make(chan error, 1)
	go func() {
		errCh <- lease2.Lock(t.Context())
	}()

	// Give time for Lock to enter waiting state.
	time.Sleep(50 * time.Millisecond)

	// Release the first lock - should notify the waiter.
	err = lease1.Unlock(t.Context())
	be.Err(t, err, nil)

	// Second lease should acquire.
	select {
	case err := <-errCh:
		be.Err(t, err, nil)
		t.Cleanup(func() {
			err := lease2.Unlock(context.Background())
			be.Err(t, err, nil)
		})
	case <-time.After(1 * time.Second):
		t.Error("Lock did not return after notification")
	}
}

func TestLease_Lock_RetriesOnTimeout(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// First lease acquires the lock with short TTL.
	{
		lease := manager.Lease(key)
		lease.TTL = 200 * time.Millisecond
		err := lease.Lock(t.Context())
		be.Err(t, err, nil)
	}

	// Second lease should retry after timeout and eventually acquire when TTL expires.
	{
		lease := manager.Lease(key)

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()

		err := lease.Lock(ctx)
		be.Err(t, err, nil)
		t.Cleanup(func() {
			err := lease.Unlock(context.Background())
			be.Err(t, err, nil)
		})
	}
}

func TestLease_Lock_MultipleWaiters(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// First lease acquires the lock.
	lease := manager.Lease(key)
	err := lease.Lock(t.Context())
	be.Err(t, err, nil)

	// Multiple waiters.
	const numWaiters = 5
	errChans := make([]chan error, numWaiters)
	leases := make([]*Lease, numWaiters)

	for i := range numWaiters {
		errChans[i] = make(chan error, 1)
		leases[i] = manager.Lease(key)
		go func() {
			defer t.Cleanup(func() { _ = leases[i].Unlock(context.Background()) })
			errChans[i] <- leases[i].Lock(t.Context())
		}()
	}

	// Give time for all to enter waiting state.
	time.Sleep(100 * time.Millisecond)

	// Release the first lock.
	err = lease.Unlock(t.Context())
	be.Err(t, err, nil)

	// Exactly one waiter should succeed.
	var successCount int
	for _, ch := range errChans {
		select {
		case err := <-ch:
			if err == nil {
				successCount++
			}
		case <-time.After(500 * time.Millisecond):
			// Waiter didn't get the lock yet, that's expected for losers.
		}
	}
	be.True(t, successCount == 1)
}

func TestLease_FencingToken_Unique(t *testing.T) {
	t.Parallel()

	key := t.Name()

	tokens := make(map[string]bool)
	for range 10_000 {
		lease := manager.Lease(key)
		be.True(t, !tokens[lease.FencingToken])
		tokens[lease.FencingToken] = true
	}
}

func TestLease_Unlock_NotOwner(t *testing.T) {
	t.Parallel()

	key := t.Name()

	// First lease acquires the lock.
	{
		lease := manager.Lease(key)
		err := lease.Lock(t.Context())
		be.Err(t, err, nil)
		t.Cleanup(func() {
			err := lease.Unlock(context.Background())
			be.Err(t, err, nil)
		})
	}

	// Second lease tries to unlock - should fail.
	{
		lease := manager.Lease(key)
		err := lease.Unlock(t.Context())
		be.Err(t, err, ErrConflict)
	}
}
