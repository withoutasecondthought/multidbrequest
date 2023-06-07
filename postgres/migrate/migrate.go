package main

import (
	"fmt"
	"github.com/ilyakaznacheev/cleanenv"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

//read env variables field TEST

type Config struct {
	Postgres `yaml:"postgres" env:"TEST"`
}

type Postgres struct {
	Host     string   `yaml:"host"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Ports    []string `yaml:"ports"`
	Name     string   `yaml:"name"`
}

type User struct {
	Id   int    `db:"id"`
	Name string `db:"name"`
	Age  int    `db:"age"`
}

type Place struct {
	Id      int    `db:"id"`
	UserId  int    `db:"user_id"`
	Name    string `db:"name"`
	Address string `db:"address"`
}

type Money struct {
	Id     int    `db:"id"`
	UserId int    `db:"user_id"`
	Amount string `db:"amount"`
	Credit string `db:"credit"`
}

func main() {
	config := new(Config)

	err := cleanenv.ReadConfig("config.yml", config)
	if err != nil {
		logrus.Fatalf("Configuration cannot be read: %v", err)
	}

	logrus.Info(config.Postgres.Ports)

	pool, err := multiDBConnect(config)
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

	var users [][]User

	users = append(users, []User{
		{Id: 3, Name: "Mark", Age: 10},
		{Id: 6, Name: "Bane", Age: 20},
		{Id: 9, Name: "Bob", Age: 50}})
	users = append(users, []User{
		{Id: 2, Name: "John", Age: 30},
		{Id: 4, Name: "Jane", Age: 20},
		{Id: 6, Name: "Mary", Age: 36},
		{Id: 8, Name: "Oleg", Age: 50}})
	users = append(users, []User{
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

	places := []Place{{
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

	moneys := []Money{{
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

func multiDBConnect(cfg *Config) ([]*sqlx.DB, error) {
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
