package main

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-oci8"
	"golang.org/x/tools/blog/atom"
	"net/http"
	"time"
)

var db *sql.DB

var connectStr = "esusr/password@//localhost:1521/xe.oracle.docker"

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

func feedHandler(rw http.ResponseWriter, req *http.Request) {
	rw.Write([]byte("handle this\n"))
}

func currentFeed() (string, error) {
	rows, err := db.Query(`select feedid from feed_state`)
	if err != nil {
		return "", err
	}

	defer rows.Close()

	var feedid string

	for rows.Next() {
		rows.Scan(&feedid)
	}

	err = rows.Err()

	return feedid, err
}

func previousFeed(feedid string) (string, error) {
	rows, err := db.Query(`select previous from feeds where feedid = :1`, feedid)
	if err != nil {
		return "", err
	}

	defer rows.Close()

	var previous string

	for rows.Next() {
		rows.Scan(&previous)
	}

	err = rows.Err()

	return previous, err
}

func topHandler(rw http.ResponseWriter, req *http.Request) {

	feedid, err := currentFeed()
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	if feedid == "" {
		http.Error(rw, "Nothing to feed yet", http.StatusNoContent)
		return
	}

	feed := atom.Feed{
		Title:   "Event store feed",
		ID:      feedid,
		Updated: atom.TimeStr(time.Now().Truncate(time.Hour).Format(time.RFC3339)),
	}

	self := atom.Link{
		Href: "http://localhost:4000/notifications/recent",
		Rel:  "self",
	}

	via := atom.Link{
		Href: fmt.Sprintf("http://localhost:4000/notifications/%s", feedid),
		Rel:  "via",
	}

	previousFeed, err := previousFeed(feedid)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	previous := atom.Link{
		Href: fmt.Sprintf("http://localhost:4000/notifications/%s", previousFeed),
		Rel:  "previous",
	}

	feed.Link = append(feed.Link, self)
	feed.Link = append(feed.Link, via)
	feed.Link = append(feed.Link, previous)

	out, err := xml.Marshal(&feed)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Write(out)
}

func main() {
	var err error
	db, err = connectToDB(connectStr)
	if err != nil {
		log.Fatalf("Error connecting to database: %s", err.Error())
	}

	r := mux.NewRouter()
	r.HandleFunc("/notifications/recent", topHandler)
	r.HandleFunc("/notifications/{feedid}", feedHandler)

	err = http.ListenAndServe(":4000", r)
	if err != nil {
		log.Fatal(err.Error())
	}
}
