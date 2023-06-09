package main

import (
	"errors"
	"github.com/jmoiron/sqlx"
	"log"
	"multidbrequest/pkg/multiconn"
	"multidbrequest/postgres/config"
	"sync"
	"time"
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

	users, err := getAllUsersWithAllFields(pool)
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
	close(ch)
	wg.Wait()
	users := FromChanelOfSlicesToSLice(ch)

	return users, nil
}

func getAllUsersWithAllFields(pool map[string]*sqlx.DB) ([]FullUser, error) {
	var users []FullUser
	var moneys []config.Money
	var places []config.Place
	usersCh := make(chan []FullUser, len(pool))
	placesCh := make(chan []config.Place, len(pool))
	moneyCh := make(chan []config.Money, len(pool))
	errorCh := make(chan error)
	wgCh := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(len(pool) + 1 + 1)
	//place
	go func() {
		var res []config.Place
		err := pool[_users_and_place].Select(&res, "SELECT * FROM places")
		if err != nil {
			errorCh <- err
			return
		} else if res != nil {
			placesCh <- res
		}
		wg.Done()
	}()
	//money
	go func() {
		var res []config.Money
		err := pool[_users_and_money].Select(&res, "SELECT * FROM money")
		if err != nil {
			errorCh <- err
			return
		} else if res != nil {
			moneyCh <- res
		}
		wg.Done()
	}()
	//users
	for _, db := range pool {
		go func(conn *sqlx.DB) {
			var res []FullUser
			err := conn.Select(&res, "SELECT * FROM users")
			if err != nil {
				errorCh <- err
				return
			} else if res != nil {
				usersCh <- res
			}
			wg.Done()
		}(db)
	}

	go func() {
		wg.Wait()
		close(wgCh)
	}()

loop:
	for {
		select {
		case err := <-errorCh:
			return nil, err
		case user := <-usersCh:
			users = append(users, user...)
		case place := <-placesCh:
			places = append(places, place...)
		case money := <-moneyCh:
			moneys = append(moneys, money...)
		case <-wgCh:
			break loop
		case <-time.After(2 * time.Second):
			return nil, errors.New("timeout")
		}
	}

	close(usersCh)
	close(placesCh)
	close(moneyCh)

	wg.Add(len(users) * 2)
	for i, user := range users {
		go func(i int, u FullUser) {
			for _, place := range places {
				if u.User.Id == place.UserId {
					users[i].Place = place
				}
			}
			wg.Done()
		}(i, user)

		go func(i int, u FullUser) {
			for _, money := range moneys {
				if u.User.Id == money.UserId {
					users[i].Money = money
				}
			}
			wg.Done()
		}(i, user)
	}
	wg.Wait()

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
