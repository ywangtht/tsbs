package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"time"
)

type dbCreator struct {
	daemonURL string
}

func (d *dbCreator) Init() {
	d.daemonURL = daemonURLs[0] // pick first one since it always exists
}

func (d *dbCreator) DBExists(dbName string) bool {
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	data := fmt.Sprintf("DROP DATABASE %s", dbName)
	resp, err := http.Post(d.daemonURL, "text/plain", bytes.NewBufferString(data))
	if err != nil {
		return fmt.Errorf("drop db error: %s", err.Error())
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("drop db returned non-200 code: %d", resp.StatusCode)
	}
	time.Sleep(time.Second)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	log.Println("CreateDB called")
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
