package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/gorilla/feeds"
	"net/http"
	"database/sql"
	_ "github.com/mattn/go-oci8"
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

func topHandler(rw http.ResponseWriter, req *http.Request) {

	feedid, err := currentFeed()
	if err != nil {
		http.Error(rw,err.Error(),http.StatusInternalServerError)
		return
	}

	if feedid == "" {
		http.Error(rw, "Nothing to feed yet", http.StatusNoContent)
		return
	}

	feed := &feeds.AtomFeed{
		Title:"Event store feed",
		Id:feedid,
		Updated:time.Now().Truncate(time.Hour).Format(time.RFC3339),
	}

	atom, err := feeds.ToXML(feed)
	rw.Write([]byte(atom))
}


func main() {
	var err error
	db,err = connectToDB(connectStr)
	if err != nil {
		log.Fatalf("Error connecting to database: %s", err.Error())
	}

	r := mux.NewRouter()
	r.HandleFunc("/notifications/{feedid}", feedHandler)
	r.HandleFunc("/notifications", topHandler)

	err = http.ListenAndServe(":4000", r)
	if err != nil {
		log.Fatal(err.Error())
	}
}
