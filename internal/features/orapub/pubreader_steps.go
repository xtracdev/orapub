package orapub

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/goes/sample/testagg"
	"github.com/xtracdev/oraeventstore"
	"github.com/xtracdev/orapub"
	"os"
	"strings"
	"database/sql"
	"github.com/xtracdev/goes"
)

var user, password, dbhost, dbPort, dbSvc string
var configErrors []string

func init() {
	var aggregateID string
	var specs []orapub.EventSpec
	var pollErr error
	var pubReadCount int
	var pubReadEvents []*goes.Event
	var publisher *orapub.OraPub

	orapub.ClearRegisteredEventProcessors()

	var eventHandler = orapub.EventProcessor{
		//TODO - take a variable number of interface{} args after db param
		Initialize: func(db *sql.DB) error {
			log.Info("pub read initialize called")
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			log.Info("pub read processor called")
			pubReadCount += 1
			pubReadEvents = append(pubReadEvents, event)
			return nil
		},
	}

	GlobalContext.BeforeAll(func() {
		log.SetLevel(log.DebugLevel)
		log.Info("Loading global context")
		user = os.Getenv("FEED_DB_USER")
		if user == "" {
			configErrors = append(configErrors, "Configuration missing FEED_DB_USER env variable")
		}

		password = os.Getenv("FEED_DB_PASSWORD")
		if password == "" {
			configErrors = append(configErrors, "Configuration missing FEED_DB_PASSWORD env variable")
		}

		dbhost = os.Getenv("FEED_DB_HOST")
		if dbhost == "" {
			configErrors = append(configErrors, "Configuration missing FEED_DB_HOST env variable")
		}

		dbPort = os.Getenv("FEED_DB_PORT")
		if dbPort == "" {
			configErrors = append(configErrors, "Configuration missing FEED_DB_PORT env variable")
		}

		dbSvc = os.Getenv("FEED_DB_SVC")
		if dbSvc == "" {
			configErrors = append(configErrors, "Configuration missing FEED_DB_SVC env variable")
		}
		log.Infof("Config errors after loading global context: %s", strings.Join(configErrors, ", "))
	})

	Given(`^Some freshly stored events$`, func() {
		if len(configErrors) != 0 {
			assert.Fail(T, strings.Join(configErrors, "\n"))
			return
		}

		os.Setenv("ES_PUBLISH_EVENTS", "1")

		ta, _ := testagg.NewTestAgg("f", "b", "b")
		ta.UpdateFoo("some new foo")
		ta.UpdateFoo("i changed my mind")
		aggregateID = ta.ID

		eventStore, err := oraeventstore.NewOraEventStore(user, password, dbSvc, dbhost, dbPort)
		assert.Nil(T, err)
		if assert.NotNil(T, eventStore) {
			err = ta.Store(eventStore)
			assert.Nil(T, err)
		}
	})

	When(`^The publish table is polled for events$`, func() {
		var connectStr = fmt.Sprintf("%s/%s@//%s:%s/%s", user, password, dbhost, dbPort, dbSvc)
		publisher = new(orapub.OraPub)
		err := publisher.Connect(connectStr, 5)
		assert.Nil(T, err)

		specs, pollErr = publisher.PollEvents(nil)
		assert.Nil(T, pollErr)

		orapub.RegisterEventProcessor("pubread", eventHandler)

		publisher.ProcessEvents(false)
	})

	Then(`^The freshly stored events are returned$`, func() {
		assert.Equal(T, pubReadCount, 3)
		assert.Equal(T, len(pubReadEvents), 3)
	})

	And(`^the event details can be retrieved$`, func() {
		for i := 0; i <= 0; i++ {
			event := pubReadEvents[i]
				assert.Equal(T, event.Source, aggregateID)
				assert.Equal(T, event.Version, i + 1)
		}

	})

	And(`^published events can be removed from the publish table$`, func() {
		publisher.ProcessEvents(false)
		assert.Equal(T, pubReadCount, 3)
		assert.Equal(T, len(pubReadEvents), 3)
	})

}
