// Package store owns the policy database's schema.
//
// The migrations are embedded in the binary rather than read from disk, and
// applied by a library rather than by a separate CLI, so that `make migrate`
// needs nothing installed that `go build` did not already fetch. A visitor
// following the case study has Go and Docker; asking them to also install a
// migration tool is how a published command stops working.
package store

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	// Registers the "pgx5" database driver under the name the DSN uses.
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/migrations"
)

// Migrator applies the schema.
type Migrator struct {
	m *migrate.Migrate
}

// NewMigrator builds one for a DSN.
func NewMigrator(dsn string) (*Migrator, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("store: reading embedded migrations: %w", err)
	}
	// The pgx/v5 driver rather than the lib/pq one, so the whole binary has a
	// single Postgres driver rather than two with different TLS behaviour.
	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+trimScheme(dsn))
	if err != nil {
		return nil, fmt.Errorf("store: opening the policy database: %w", err)
	}
	return &Migrator{m: m}, nil
}

// trimScheme normalises a DSN so either postgres:// or postgresql:// works,
// which is the difference between two tools' documentation and not something
// an operator should have to notice.
func trimScheme(dsn string) string {
	for _, prefix := range []string{"postgres://", "postgresql://", "pgx5://", "pgx://"} {
		if len(dsn) >= len(prefix) && dsn[:len(prefix)] == prefix {
			return dsn[len(prefix):]
		}
	}
	return dsn
}

// Up applies every pending migration and reports how many ran.
//
// Applying nothing is success, not an error: `make migrate` is run again on
// every startup in some deployments, and a command that fails when there is
// nothing to do cannot be used that way.
func (m *Migrator) Up() (applied bool, err error) {
	err = m.m.Up()
	if errors.Is(err, migrate.ErrNoChange) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: applying migrations: %w", err)
	}
	return true, nil
}

// Down rolls back n steps, or everything when n is 0.
func (m *Migrator) Down(n int) error {
	var err error
	if n <= 0 {
		err = m.m.Down()
	} else {
		err = m.m.Steps(-n)
	}
	if errors.Is(err, migrate.ErrNoChange) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: rolling back: %w", err)
	}
	return nil
}

// Version reports the applied version and whether the schema is dirty.
//
// Dirty means a migration failed part-way and the database is in a state no
// migration describes. It is reported rather than repaired: guessing which
// half ran is how a schema quietly diverges between environments.
func (m *Migrator) Version() (version uint, dirty bool, err error) {
	version, dirty, err = m.m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: reading schema version: %w", err)
	}
	return version, dirty, nil
}

// Close releases the migrator's connections.
func (m *Migrator) Close() error {
	srcErr, dbErr := m.m.Close()
	return errors.Join(srcErr, dbErr)
}
