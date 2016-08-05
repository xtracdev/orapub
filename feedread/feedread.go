package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/xml"
	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-oci8"
	"golang.org/x/tools/blog/atom"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

var db *sql.DB
var connectStr = "replicantusr/password@//localhost:1521/xe.oracle.docker"
var recentItemsUrl = "http://localhost:5000/notifications/recent"

func extractPrevious(fb []byte) (string, error) {
	var feed atom.Feed

	err := xml.Unmarshal(fb, &feed)
	if err != nil {
		return "", err
	}

	var previous string
	for _, link := range feed.Link {
		if link.Rel == "previous" {
			previous = link.Href
			break
		}
	}

	return previous, nil
}

func lineUpFeeds() ([]string, error) {
	var feedUrl = recentItemsUrl
	var feeds []string

	for {
		log.Infof("look at %s", feedUrl)
		resp, err := http.Get(feedUrl)
		if err != nil {
			return feeds, nil
		}

		defer resp.Body.Close()

		feedBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return feeds, err
		}

		previous, err := extractPrevious(feedBytes)
		if err != nil {
			return feeds, err
		}

		if previous == "" {
			break
		}

		feeds = append(feeds, previous)
		feedUrl = previous
	}

	return feeds, nil
}

func connectToDB(connectStr string) (*sql.DB, error) {
	db, err := sql.Open("oci8", connectStr)
	if err != nil {
		log.Warnf("Error connecting to oracle: %s", err.Error())
		return nil, err
	}

	//Are we really in an ok state for starters?
	err = db.Ping()
	if err != nil {
		log.Infof("Error connecting to oracle: %s", err.Error())
		return nil, err
	}

	return db, nil
}

//TODO - error handling strategy - maybe keep a list of the stuff we couldn't load, retry it
//or log it for later processing.
func processFeed(feedUrl string) {
	log.Infof("look at %s", feedUrl)
	resp, err := http.Get(feedUrl)
	if err != nil {
		log.Warnf("Error reading feed %s", err.Error())
		return
	}

	defer resp.Body.Close()

	fb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warnf("Error reading feed bytes - %s", err.Error())
		return
	}

	var feed atom.Feed

	err = xml.Unmarshal(fb, &feed)
	if err != nil {
		log.Warnf("Error unmarshalling feed bytes - %s", err.Error())
		return
	}

	for _, e := range feed.Entry {
		storeEntry(e)
	}

}

func storeEntry(e *atom.Entry) {
	//format is urn:esid:aggid:version
	idParts := strings.Split(e.ID, ":")
	if len(idParts) != 4 {
		log.Warn("Expected entity id %s to have 4 parts, got %d", e.ID, len(idParts))
		return
	}
	aggId := idParts[2]

	version, err := strconv.Atoi(idParts[3])
	if err != nil {
		log.Warn("Error converting version: %s", err.Error())
		return
	}

	payload, err := base64.StdEncoding.DecodeString(e.Content.Body)
	if err != nil {
		log.Warn("Error decoding payload: %s", err.Error())
		return
	}

	_, err = db.Exec("insert into events (aggregate_id, version, typecode, payload) values (:1,:2,:3,:4)",
		aggId, version, e.Content.Type, payload)
	if err != nil {
		log.Warnf("Error adding entry to events table: %s", err)
	}
}

func main() {
	var err error
	db, err = connectToDB(connectStr)
	if err != nil {
		log.Fatalf("Error connecting to database: %s", err.Error())
	}

	//Start at with the latest, navigate to the start, then read going forward. Use a
	//caching reverse proxy to minimize network traffic and database hits.
	feeds, err := lineUpFeeds()
	if err != nil {
		log.Fatalf("Error lining up feeds: %s", err.Error())
	}

	//Got the feeds, in reverse order. Read them and load up the db
	for i := len(feeds) - 1; i >= 0; i-- {
		log.Infof("load up %s", feeds[i])
		processFeed(feeds[i])
	}

}
