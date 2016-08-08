package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/alecthomas/kingpin"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-oci8"
	"golang.org/x/tools/blog/atom"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	app          = kingpin.New("feedproxy", "Reverse caching proxy for event feed")
	linkhostport = app.Flag("linkhostport", "host:port to use in link urls").Required().String()
)

var db *sql.DB

type EventStoreContent struct {
	XMLName     xml.Name  `xml:"http://xtraclabs.com/eventstore event"`
	AggregateId string    `xml:"aggregateId"`
	Version     int       `xml:"version"`
	Published   time.Time `xml:"published"`
	TypeCode    string    `xml:"typecode"`
	Content     string    `xml:"content"`
}

var connectStr = "esusr/password@//localhost:1521/xe.oracle.docker"
var ErrNoSuchFeed = errors.New("Unknown feed specified")

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

func lastUpdateForFeed(feedid string) (time.Time, error) {
	var lastUpdate time.Time

	rows, err := db.Query(`select event_time from feed_data where feedid = :1 and rownum = 1 order by id desc`, feedid)
	if err != nil {
		return lastUpdate, err
	}

	defer rows.Close()

	for rows.Next() {
		rows.Scan(&lastUpdate)
	}

	err = rows.Err()

	return lastUpdate, err
}

func addItemsToFeed(feed *atom.Feed) error {

	log.Infof("reading feed_data for feed %s", feed.ID)
	rows, err := db.Query(`select event_time, aggregate_id, version, typecode, payload from feed_data where feedid = :1 order by id desc`,
		feed.ID)
	if err != nil {
		return err
	}

	defer rows.Close()

	var eventTime time.Time
	var aggregateID string
	var version int
	var typecode string
	var payload []byte

	log.Info("scanning rows")
	for rows.Next() {
		rows.Scan(&eventTime, &aggregateID, &version, &typecode, &payload)

		encodedPayload := base64.StdEncoding.EncodeToString([]byte(payload))

		content := &atom.Text{
			Type: typecode,
			Body: encodedPayload,
		}

		entry := &atom.Entry{
			Title:     "event",
			ID:        fmt.Sprintf("urn:esid:%s:%d", aggregateID, version),
			Published: atom.TimeStr(eventTime.Format(time.RFC3339Nano)),
			Content:   content,
		}

		link := atom.Link{
			Rel:  "self",
			Href: fmt.Sprintf("http://%s/notifications/%s/%d", *linkhostport, aggregateID, version),
		}

		entry.Link = append(entry.Link, link)

		feed.Entry = append(feed.Entry, entry)

	}

	err = rows.Err()

	return nil
}

func feedHandler(rw http.ResponseWriter, req *http.Request) {

	feedid := mux.Vars(req)["feedid"]
	if feedid == "" {
		http.Error(rw, "No feed id in uri", http.StatusInternalServerError)
		return
	}

	log.Infof("processing request for feed %s", feedid)

	//Look up previous
	log.Infof("look up feed %s", feedid)
	feedIdFromDB, previousFeed, err := lookupFeed(feedid)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	if feedIdFromDB == "" {
		http.Error(rw, "", http.StatusNotFound)
		return
	}

	log.Infof("previous feed is %s", previousFeed)

	next, feedIdFromDB, err := lookupNext(feedid)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("next feed is %s", next)

	updated, err := lastUpdateForFeed(feedid)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("last update was %s", updated)

	feed := atom.Feed{
		Title:   "Event store feed",
		ID:      feedid,
		Updated: atom.TimeStr(updated.Format(time.RFC3339)),
	}

	self := atom.Link{
		Href: fmt.Sprintf("http://%s/notifications/%s", *linkhostport, feedid),
		Rel:  "self",
	}

	feed.Link = append(feed.Link, self)

	if previousFeed != "" {
		feed.Link = append(feed.Link, atom.Link{
			Href: fmt.Sprintf("http://%s/notifications/%s", *linkhostport, previousFeed),
			Rel:  "previous",
		})
	}

	if next != "" {
		feed.Link = append(feed.Link, atom.Link{
			Href: fmt.Sprintf("http://%s/notifications/%s", *linkhostport, next),
			Rel:  "next",
		})
	}

	addItemsToFeed(&feed)

	out, err := xml.Marshal(&feed)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	//For all feeds except recent, we can indicate the page can be cached for a long time,
	//e.g. 30 days. The recent page is mutable so we don't indicate caching for it. We could
	//potentially attempt to load it from this method via link traversal.
	if next != "" {
		log.Infof("setting Cache-Control max-age=2592000 for ETag %s", feedid)
		rw.Header().Add("Cache-Control", "max-age=2592000") //Contents are immutable, cache for a month
		rw.Header().Add("ETag", feedid)
	}

	rw.Header().Add("Content-Type", "application/atom+xml")
	rw.Write(out)

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

func lookupFeed(id string) (string, string, error) {

	rows, err := db.Query(`select feedid, previous from feeds where feedid = :1`, id)
	if err != nil {
		return "", "", err
	}

	defer rows.Close()

	var feedid string
	var previous sql.NullString

	log.Infof("Read query results")
	for rows.Next() {
		rows.Scan(&feedid, &previous)
	}

	log.Infof("...previous null? %t", previous.Valid)
	log.Infof("...read feedid %s, previous %s", feedid, previous.String)

	err = rows.Err()

	return feedid, previous.String, err
}

func lookupNext(id string) (string, string, error) {
	rows, err := db.Query(`select feedid, previous from feeds where previous = :1`, id)
	if err != nil {
		return "", "", err
	}

	defer rows.Close()

	var feedid string
	var previous string

	for rows.Next() {
		rows.Scan(&feedid, &previous)
	}

	err = rows.Err()

	return feedid, previous, err
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
		Href: fmt.Sprintf("http://%s/notifications/recent", *linkhostport),
		Rel:  "self",
	}

	via := atom.Link{
		Href: fmt.Sprintf("http://%s/notifications/%s", *linkhostport, feedid),
		Rel:  "via",
	}

	_, previousFeed, err := lookupFeed(feedid)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	previous := atom.Link{
		Href: fmt.Sprintf("http://%s/notifications/%s", *linkhostport, previousFeed),
		Rel:  "previous",
	}

	feed.Link = append(feed.Link, self)
	feed.Link = append(feed.Link, via)
	feed.Link = append(feed.Link, previous)

	addItemsToFeed(&feed)

	out, err := xml.Marshal(&feed)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Add("Content-Type", "application/atom+xml")
	rw.Write(out)
}

func readEntryFromDB(aggId string, version int) (time.Time, string, []byte, error) {
	var eventTime time.Time
	var typecode string
	var payload []byte

	rows, err := db.Query(`select event_time, typecode, payload from feed_data where aggregate_id = :1 and version = :2`,
		aggId, version)
	if err != nil {
		return eventTime, typecode, payload, err
	}

	defer rows.Close()

	for rows.Next() {
		rows.Scan(&eventTime, &typecode, &payload)
	}

	err = rows.Err()

	return eventTime, typecode, payload, err
}

func entryHandler(rw http.ResponseWriter, req *http.Request) {
	//Note: this handler will not be called by the mux if aggregate id or version are empty
	aggregateId := mux.Vars(req)["aggregateId"]
	versionParam := mux.Vars(req)["version"]

	version, err := strconv.Atoi(versionParam)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	eventTime, typecode, payload, err := readEntryFromDB(aggregateId, version)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	eventContent := EventStoreContent{
		AggregateId: aggregateId,
		Version:     version,
		TypeCode:    typecode,
		Published:   eventTime,
		Content:     base64.StdEncoding.EncodeToString(payload),
	}

	marshalled, err := xml.Marshal(&eventContent)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Add("Content-Type", "application/xml")
	rw.Header().Add("ETag", fmt.Sprintf("%s:%d", aggregateId, version))
	rw.Header().Add("Cache-Control", "max-age=2592000")
	rw.Write(marshalled)
}

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	var err error
	db, err = connectToDB(connectStr)
	if err != nil {
		log.Fatalf("Error connecting to database: %s", err.Error())
	}

	r := mux.NewRouter()
	r.HandleFunc("/notifications/recent", topHandler)
	r.HandleFunc("/notifications/{feedid}", feedHandler)
	r.HandleFunc("/notifications/{aggregateId}/{version}", entryHandler)

	err = http.ListenAndServe(":4000", r)
	if err != nil {
		log.Fatal(err.Error())
	}
}
