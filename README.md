# Warden

[![CI](https://github.com/denpeshkov/warden/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/denpeshkov/warden/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/denpeshkov/warden)](https://goreportcard.com/report/github.com/denpeshkov/warden)
[![Go Reference](https://pkg.go.dev/badge/github.com/denpeshkov/warden.svg)](https://pkg.go.dev/github.com/denpeshkov/warden)

Warden is a library that provides distributed leases for Go backed by Redis and PostgreSQL.

# Features

- **Multiple Backends**: Choose between Redis or PostgreSQL based on your infrastructure
- **Blocking & Non-blocking Acquisition**: `Lock()` waits for lease availability, `TryLock()` returns immediately
- **Automatic Expiration**: Leases automatically expire after TTL to prevent deadlocks
- **Lease Renewal**: Extend lease lifetime while work is in progress
- **Fencing Tokens**: Prevent conflicts when multiple nodes compete for the same lease
- **Context-aware**: Full support for context cancellation and timeouts
- **Thread-safe**: Safe for concurrent use by multiple goroutines

# Installation

```bash
go get github.com/denpeshkov/warden
```

# Usage

## Redis Backend

```go
package main

import (
	"context"
	"log"

	warden "github.com/denpeshkov/warden/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer func() { _ = rdb.Close() }()

	// Create lease manager
	manager := warden.NewManager(rdb)

	// Create a lease for a specific resource
	lease := manager.Lease("my-task")

	// Acquire the lease
	ctx := context.Background()
	if err := lease.Lock(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := lease.Unlock(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// Do your work while holding the lease
	// ...
}
```

## PostgreSQL Backend

```go
package main

import (
	"context"
	"log"

	"github.com/denpeshkov/warden/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// Create PostgreSQL connection pool
	pool, err := pgxpool.New(context.Background(),
		"postgres://user:password@localhost:5432/mydb?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	// Create the lease table (run once)
	if _, err = pool.Exec(context.Background(), postgres.CreateLockTableSQL); err != nil {
		log.Fatal(err)
	}

	// Create lease manager
	manager := postgres.NewManager(pool)

	// Create a lease for a specific resource
	lease := manager.Lease("my-task")

	// Acquire the lease
	ctx := context.Background()
	if err := lease.Lock(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := lease.Unlock(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// Do your work while holding the lease
	// ...
}
```
