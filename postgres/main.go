package main

import (
	"github.com/jmoiron/sqlx"
	"log"
	"multidbrequest/pkg/multiconn"
	"multidbrequest/postgres/config"
	"sync"
)

const (
	_users           = "users"
	_users_and_money = "users_money"
	_users_and_place = "users_places"
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

	_, ok := pool[_users]
	if !ok {
		log.Fatalf("Database %s not found", _users)
	}

	_, ok = pool[_users_and_money]
	if !ok {
		log.Fatalf("Database %s not found", _users_and_money)
	}

	_, ok = pool[_users_and_place]
	if !ok {
		log.Fatalf("Database %s not found", _users_and_place)
	}

	users, err := getAllUsers(pool)
	if err != nil {
		log.Fatalf("Get all users failed: %v", err)
	}

	for _, user := range users {
		log.Printf("%+v", user)
	}

}

func getAllUsers(pool map[string]*sqlx.DB) ([]FullUser, error) {
	ch := make(chan []FullUser, len(pool))
	var err error
	var wg sync.WaitGroup
	wg.Add(len(pool))
	for _, db := range pool {
		go func(conn *sqlx.DB) {
			var res []FullUser
			err = conn.Select(&res, "SELECT * FROM users")
			if err != nil {
				return
			}

			ch <- res
			wg.Done()
		}(db)
		if err != nil {
			return nil, err
		}
	}

	wg.Wait()
	users := FromChanelOfSlicesToSLice(ch)

	close(ch)
	return users, nil
}

func FromChanelOfSlicesToSLice[T any](ch chan []T) []T {
	var res []T
	length := len(ch)

	for length > 0 {
		res = append(res, <-ch...)
		length--
	}
	return res
}
