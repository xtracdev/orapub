package main


import (
	log "github.com/Sirupsen/logrus"
	"fmt"
	"database/sql"
	_ "github.com/mattn/go-oci8"
	"time"
	"crypto/rand"
	"github.com/xtraclabs/orapub"
	"github.com/xtraclabs/goes"
)

var connectStr = "esusr/password@//localhost:1521/xe.oracle.docker"

type feedState struct {
	feedid string
	year int
	month int
	day int
	hour int
}

func connectToDB(connectStr string)(*sql.DB,error) {
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

func readFeedState(db *sql.DB)(*feedState, error) {
	rows, err := db.Query(`select feedid, year, month, day, hour from feed_state`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var feedid string
	var year int
	var month int
	var day int
	var hour int

	var fs *feedState

	for rows.Next() {
		rows.Scan(&feedid,&year,&month,&day,&hour)

		fs = &feedState{
			feedid: feedid,
			year:year,
			month:month,
			day:day,
			hour:hour,
		}
	}

	err = rows.Err()

	return fs, err
}

func uuid() (string,error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "",err
	}

	return fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]),nil

}

func initFeedState(db *sql.DB) (*feedState,error) {
	now := time.Now()
	urn,err := uuid()
	if err != nil {
		return nil,err
	}

	//Being a little sloppy here - a merge would be better, and the below should probably be in
	//a single transaction too.
	_,err = db.Exec("delete from feed_state")
	if err != nil {
		return nil, err
	}

	fs := &feedState{
		feedid:urn,
		year:now.Year(),
		month:int(now.Month()),
		day:now.Day(),
		hour:now.Hour(),
	}

	_,err = db.Exec("insert into feed_state (feedid, year, month, day, hour) values (:1,:2,:3,:4,:5)",
		fs.feedid,fs.year,fs.month,fs.day,fs.hour)

	return fs,err
}

func updateFeedStateIfNeeded(db *sql.DB, fs *feedState) (*feedState,error) {
	now := time.Now().Truncate(time.Hour)

	var updateNeeded bool

	if(now.Year() > fs.year) {
		updateNeeded = true
	} else if (int(now.Month()) > fs.month) {
		updateNeeded = true
	} else if (now.Day() > fs.day) {
		updateNeeded = true
	} else if (now.Hour() > fs.hour) {
		updateNeeded = true
	}

	if updateNeeded == false {
		return fs,nil
	}

	log.Info("updating feed state")

	return initFeedState(db)
}

func main() {
	log.Info("pub2feed alive")

	log.Info("connect to database")

	db, err := connectToDB(connectStr)
	if err != nil {
		log.Fatalf("Error connecting to db: %s", err.Error())
	}

	defer db.Close()

	err = processRecords(db)
	if err != nil {
		log.Fatalf("Unable to process records: %s", err.Error())
	}
}

func processRecords(db *sql.DB) error {

	publisher := orapub.NewOraPub()
	err := publisher.Connect(connectStr)
	if err != nil {
		log.Warnf("Unable to connect publisher reader")
		return err
	}



	fs,err := readFeedState(db)
	if err != nil {
		log.Warnf("Error reading feed state: %s", err.Error())
		return err
	}

	if fs == nil {
		log.Info("No feed state read")
		fs,err = initFeedState(db)
		if err != nil {
			log.Warnf("Error initializing feed state: %s", err.Error())
		}
		return err
	}

	publisher.RegisterEventProcessor("feed data writer", func(event *goes.Event) error {
		log.Infof("processing %s.%d", event.Source, event.Version)
		_,err = db.Exec("insert into feed_data (feedid, aggregate_id, version,typecode, payload) values(:1,:2,:3,:4,:5)",
			fs.feedid, event.Source, event.Version, event.TypeCode, event.Payload)
		return err
	})

	for {



		log.Infof("Feed state - %v",*fs)
		fs,err = updateFeedStateIfNeeded(db, fs)
		if err != nil {
			log.Warnf("Unable to update feed state: %s",err.Error())
			continue
		}

		es, err := publisher.PollEvents()
		if err != nil {
			log.Warnf("Error polling for events: %s", err.Error())
			continue
		}

		log.Infof("Process %d events", len(es))
		for _,eventContext := range es {

			e, err := publisher.RetrieveEventDetail(eventContext.AggregateId, eventContext.Version)
			if err != nil {
				log.Warnf("Error reading event to process (%v): %s",eventContext, err)
				continue
			}

			//TODO - make error codes available to interested users of OraPub
			publisher.ProcessEvent(e)
		}

		log.Infof("Deleting %d events", len(es))
		err = publisher.DeleteProcessedEvents(es)
		if err != nil {
			log.Warnf("Error cleaning up processed events: %s", err)
		}

		if len(es) == 0 {
			log.Infof("Nothing to do... time for a 5 second sleep")
			time.Sleep(5 * time.Second)
		}
		
	}
}
