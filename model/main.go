package main

import (
	"github.com/sirupsen/logrus"
	"sort"
	"time"
)

func main() {
	var dbPool = make([]Database, 3)

	dbPool[0] = Database{
		Data: []PersonFromDB{
			{Id: 3, Name: "Mark", Age: 10},
			{Id: 6, Name: "Bane", Age: 20},
			{Id: 9, Name: "Bob", Age: 50},
		},
		requestTime: 5 * time.Second,
	}
	dbPool[1] = Database{
		Data: []PersonFromDB{
			{Id: 2, Name: "John", Age: 30},
			{Id: 4, Name: "Jane", Age: 20},
			{Id: 6, Name: "Mary", Age: 36},
			{Id: 8, Name: "Oleg", Age: 50},
		},
		requestTime: 150 * time.Millisecond,
	}
	dbPool[2] = Database{
		Data: []PersonFromDB{
			{Id: 0, Name: "Kate", Age: 22},
			{Id: 101, Name: "Egor", Age: 39},
			{Id: 5, Name: "Anatoliy", Age: 21},
			{Id: 7, Name: "Alena", Age: 54},
		},
		requestTime: 1 * time.Second,
	}

	response := makeRequest(dbPool)

	sortedResponse := sortPersonsById(response)

	logrus.Info(sortedResponse)
}

func makeRequest(pool []Database) []PersonFromDB {
	var returnList []PersonFromDB
	var reqCh = make(chan []PersonFromDB, len(pool))

	for _, c := range pool {
		go func(conn Database) {
			reqCh <- conn.GetData()
		}(c)
	}

	for range pool {
		returnList = append(returnList, <-reqCh...)
	}

	return returnList
}

func sortPersonsById(persons []PersonFromDB) []PersonFromDB {
	var sortedPersons []PersonFromDB
	var ids = make([]int, len(persons))
	var mapPersons = make(map[int]PersonFromDB)
	for i, p := range persons {
		mapPersons[p.Id] = p
		ids[i] = p.Id
	}

	sort.Ints(ids)

	for _, id := range ids {
		sortedPersons = append(sortedPersons, mapPersons[id])
	}

	return sortedPersons
}

type PersonFromDB struct {
	Id   int
	Name string
	Age  int
}

type Database struct {
	Data        []PersonFromDB
	requestTime time.Duration
}

func (db *Database) GetData() []PersonFromDB {
	time.Sleep(db.requestTime)
	return db.Data
}
