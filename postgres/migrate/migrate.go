package main

import (
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"
	"log"
	"multidbrequest/pkg/multiconn"
	"multidbrequest/postgres/config"
)

//read env variables field TEST

func main() {
	cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}
	logrus.Info(cfg.Postgres.Ports)

	pool, err := multiconn.MultiPostgresDBConnect(cfg)
	if err != nil {
		logrus.Fatalf("DB connection failed: %v", err)
	}

	generalTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL, age INT NOT NULL)", "users")

	for id, db := range pool {
		_, err = db.Exec(generalTable)
		if err != nil {
			logrus.Fatalf("DB %d table creation failed: %v", id, err)
		}
	}

	var users [][]config.User

	users = append(users, []config.User{
		{Id: 3, Name: "Mark", Age: 10},
		{Id: 6, Name: "Bane", Age: 20},
		{Id: 9, Name: "Bob", Age: 50}})
	users = append(users, []config.User{
		{Id: 2, Name: "John", Age: 30},
		{Id: 4, Name: "Jane", Age: 20},
		{Id: 6, Name: "Mary", Age: 36},
		{Id: 8, Name: "Oleg", Age: 50}})
	users = append(users, []config.User{
		{Id: 0, Name: "Kate", Age: 22},
		{Id: 101, Name: "Egor", Age: 39},
		{Id: 5, Name: "Anatoliy", Age: 21},
		{Id: 7, Name: "Alena", Age: 54}})

	for id, db := range pool {
		tx := db.MustBegin()

		for _, user := range users[id] {
			_, err := tx.NamedExec("INSERT INTO users (id, name, age) VALUES (:id, :name, :age)", &user)
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					logrus.Fatalf("update failed: %v, unable to rollback: %v\n", err, rollbackErr)
				}
				logrus.Fatalf("update failed: %v\n", err)
			}
		}
		if err := tx.Commit(); err != nil {
			logrus.Fatalf("commit failed: %v\n", err)
		}
	}

	placesTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, user_id INT, name TEXT NOT NULL, address TEXT NOT NULL)", "places")

	places := []config.Place{{
		UserId:  2,
		Name:    "Argentina",
		Address: "Some address",
	}, {
		UserId:  6,
		Name:    "Mexico",
		Address: "Some cock address",
	}, {
		UserId:  1,
		Name:    "Russia",
		Address: "Some GOod address",
	}}

	placesTx := pool[0].MustBegin()
	_, err = placesTx.Exec(placesTable)
	if err != nil {
		if err := placesTx.Rollback(); err != nil {
			logrus.Fatalf("DB %d table rollback failed: %v", err)
		}
		logrus.Fatalf("DB %d table creation failed: %v", err)
	}

	for _, place := range places {
		_, err := placesTx.NamedExec("INSERT INTO places (user_id, name, address) VALUES (:user_id, :name, :address)", place)
		if err != nil {
			if err := placesTx.Rollback(); err != nil {
				logrus.Fatalf("DB %d table rollback failed: %v", err)
			}
			logrus.Fatalf("error insert into places: %v", err)
		}
	}

	err = placesTx.Commit()
	if err != nil {
		logrus.Fatalf("failed to commit: %v", err)
	}

	moneyTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, user_id INT, amount TEXT NOT NULL, credit TEXT NOT NULL)", "money")

	moneys := []config.Money{{
		UserId: 2,
		Amount: "100$",
		Credit: "less than you can imagine",
	}, {
		UserId: 6,
		Amount: "200RUB",
		Credit: "oh no your credit is poof",
	}, {
		UserId: 1,
		Amount: "300bucks",
		Credit: "world cumshot",
	}}

	moneysTx := pool[2].MustBegin()
	_, err = moneysTx.Exec(moneyTable)
	if err != nil {
		if err := moneysTx.Rollback(); err != nil {
			logrus.Fatalf("DB %d table rollback failed: %v", err)
		}
		logrus.Fatalf("DB %d table creation failed: %v", err)
	}

	for _, money := range moneys {
		_, err := moneysTx.NamedExec("INSERT INTO money (user_id, amount, credit) VALUES (:user_id, :amount, :credit)", money)
		if err != nil {
			if err := moneysTx.Rollback(); err != nil {
				logrus.Fatalf("DB %d table rollback failed: %v", err)
			}
			logrus.Fatalf("error insert into places: %v", err)
		}
	}

	err = moneysTx.Commit()
	if err != nil {
		logrus.Fatalf("failed to commit: %v", err)
	}
}
