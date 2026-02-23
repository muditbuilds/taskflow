package store

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrate runs SQL migration statements. Pass the contents of a schema file.
func Migrate(ctx context.Context, pool *pgxpool.Pool, sql []byte) error {
	for _, stmt := range strings.Split(string(sql), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
