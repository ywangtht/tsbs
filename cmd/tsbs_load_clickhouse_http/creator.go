package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

type dbCreator struct {
	daemonURL string
}

func (d *dbCreator) Init() {
	d.daemonURL = daemonURLs[0] // pick first one since it always exists
}

func (d *dbCreator) DBExists(dbName string) bool {
	data := "exists table benchmark.benchmark_table"
	req, err := http.NewRequest("POST", d.daemonURL, bytes.NewBufferString(data))
	if err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	log.Println("response of DBExists")
	log.Println(resp)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return false
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	bodyString := string(bodyBytes)
	if strings.TrimSpace(bodyString) == "1" {
		log.Println("table exists")
		return true
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	log.Println("Removing DB: " + dbName)
	data := fmt.Sprintf("DROP DATABASE %s", dbName)
	http.Post(d.daemonURL, "text/plain", bytes.NewBufferString(data))
	/*
		if err != nil {
			return fmt.Errorf("drop db error: %s", err.Error())
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("drop db returned non-200 code: %d", resp.StatusCode)
		}
	*/
	time.Sleep(time.Second)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	log.Println("Creating DB: " + dbName)
	data := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	req, err := http.NewRequest("POST", d.daemonURL, bytes.NewBufferString(data))
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	log.Println(resp)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad db create")
	}

	time.Sleep(time.Second)
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	log.Println("Creating DB table")
	createTableCmd := `CREATE TABLE benchmark.benchmark_table
	(
			phRecvTime      Datetime,
			phCustId        UInt32,
			customer        String,
			eventType       String,
			reptDevIpAddr   IPv4,
			reptDevName     String,
			rawEventMsg     String,
			eventId         UInt64,
			phEventCategory     UInt16,
			eventSeverityCat    LowCardinality(String),
			reptVendor          String,
			reptModel           String,
			parserName          LowCardinality(String),
			eventParsedOk       UInt8,
			collectorId         UInt32,
			metrics_string Nested (
					name LowCardinality(String),
					value String
			),
			metrics_datetime Nested (
					name LowCardinality(String),
					value Datetime
			),
			metrics_ipv4 Nested (
					name LowCardinality(String),
					value IPv4
			),
			metrics_ipv6 Nested (
					name LowCardinality(String),
					value IPv6
			),
			metrics_uint8 Nested (
					name LowCardinality(String),
					value UInt8
			),
			metrics_uint16 Nested (
					name LowCardinality(String),
					value UInt16
			),
			metrics_uint32 Nested (
					name LowCardinality(String),
					value UInt32
			),
			metrics_uint64 Nested (
					name LowCardinality(String),
					value UInt64
			),
			metrics_int16 Nested (
					name LowCardinality(String),
					value Int16
			),
			metrics_int32 Nested (
					name LowCardinality(String),
					value Int32
			),
			metrics_int64 Nested (
					name LowCardinality(String),
					value Int64
			),
			metrics_float64 Nested (
					name LowCardinality(String),
					value Float64
			)
	) ENGINE = MergeTree()
	PARTITION BY toYYYYMMDD(phRecvTime)
	ORDER BY (phCustId, eventType, phRecvTime)
	SETTINGS index_granularity = 8192`

	req, err := http.NewRequest("POST", d.daemonURL, bytes.NewBufferString(createTableCmd))
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	log.Println(resp)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad table create")
	}

	if err != nil {
		return err
	}
	return nil
}
