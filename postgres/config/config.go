package config

import (
	"github.com/ilyakaznacheev/cleanenv"
)

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

func InitConfig() (*Config, error) {
	config := new(Config)

	err := cleanenv.ReadConfig("config.yml", config)
	return config, err
}
