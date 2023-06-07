package main

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"log"
	"multidbrequest/pkg/multiconn"
	"multidbrequest/postgres/config"
)

type FullUser struct {
	config.User
	config.Money
	config.Place
}

func main() {
	cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}

	pool, err := multiconn.Postgres(cfg)
	if err != nil {
		log.Fatalf("Postgres pool initialization failed: %v", err)
	}

	users := getAllUsers(pool)

	for _, user := range users {
		logrus.Infof("User: %+v", user)
	}
}

func getAllUsers(pool []*sqlx.DB) []FullUser {
	var users []FullUser
	ch := make(chan []FullUser, len(pool))
	for _, db := range pool {
		go func(conn *sqlx.DB) {
			var res []FullUser
			err := conn.Select(&res, "SELECT * FROM users")
			if err != nil {
				logrus.Errorf("Select failed: %v", err)
			} else {
				ch <- res
			}
		}(db)
	}

	for range pool {
		users = append(users, <-ch...)
	}

	return users
}
