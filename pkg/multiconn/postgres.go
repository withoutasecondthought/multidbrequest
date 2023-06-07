package multiconn

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"multidbrequest/postgres/config"
)

func Postgres(cfg *config.Config) ([]*sqlx.DB, error) {
	pool := make([]*sqlx.DB, len(cfg.Postgres.Ports))

	for i, port := range cfg.Postgres.Ports {
		db, err := sqlx.Open("pgx", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Postgres.Host, port, cfg.Postgres.User, cfg.Postgres.Password, cfg.Postgres.Name))
		if err != nil {
			return nil, err
		}
		pool[i] = db
	}

	return pool, nil
}
