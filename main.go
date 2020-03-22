package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type PaxResponse []struct {
	Altitude  int         `json:"altitude"`
	Ble       interface{} `json:"ble"`
	DeviceID  string      `json:"device_id"`
	Hdop      float64     `json:"hdop"`
	Latitude  float64     `json:"latitude"`
	Longitude float64     `json:"longitude"`
	Pax       json.Number `json:"pax"`
	Raw       string      `json:"raw"`
	Sats      int         `json:"sats"`
	Time      time.Time   `json:"time"`
	Wifi      interface{} `json:"wifi"`
}

var (
	host     string
	port     string
	user     string
	password string
	dbname   string
	ttnauth  string
	ttnurl   string
)

func main() {
	host = os.Getenv("host")
	port = os.Getenv("port")
	user = os.Getenv("user")
	password = os.Getenv("password")
	dbname = os.Getenv("dbname")
	ttnauth = os.Getenv("ttnauth")
	ttnurl = os.Getenv("ttnurl")

	for {
		paxinfo := calltnt()
		calldb(paxinfo)

		log.Println("wait for 5 min")
		time.Sleep(5 * time.Minute)
	}
}

func calltnt() PaxResponse {
	url := ttnurl
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", ttnauth)

	res, err := client.Do(req)
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	PaxResponse := PaxResponse{}
	err = json.Unmarshal(body, &PaxResponse)
	if err != nil {
		fmt.Println(err)
	}

	return PaxResponse
}

func calldb(PaxResponse PaxResponse) {
	sqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?tls=skip-verify&autocommit=true",
		user, password, host, port, dbname)

	db, err := sql.Open("mysql", sqlInfo)
	if err != nil {
		log.Println(err)
	}
	defer db.Close()

	sqlStatement := "INSERT INTO `sichereseinkaufen`.`market_pax` (`market_id`, `timestamp`, `pax_count`, `average_presence_time`)	VALUES (?, ?, ?, ?)"

	var pax int
	var errl error
	if pax, errl = strconv.Atoi(PaxResponse[0].Pax.String()); errl != nil {
		pax = 0
	}

	insert, errq := db.Query(sqlStatement, 32, PaxResponse[0].Time, pax, 0)
	if errq != nil {
		log.Println(errq)
	}
	insert.Close()
}
