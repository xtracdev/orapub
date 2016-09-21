package pubreader

import (
	"fmt"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/goes/sample/testagg"
	"github.com/xtracdev/oraeventstore"
	"github.com/xtracdev/orapub"
	"os"
	"strings"
)



func init() {
	var aggregateID string
	var specs []orapub.EventSpec
	var pollErr error
	var eventPublisher *orapub.OraPub

	var configErrors []string

	user := os.Getenv("FEED_DB_USER")
	if user == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_USER env variable")
	}

	password := os.Getenv("FEED_DB_PASSWORD")
	if password == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_PASSWORD env variable")
	}

	dbhost := os.Getenv("FEED_DB_HOST")
	if dbhost == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_HOST env variable")
	}

	dbPort := os.Getenv("FEED_DB_PORT")
	if dbPort == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_PORT env variable")
	}

	dbSvc := os.Getenv("FEED_DB_SVC")
	if dbSvc == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_SVC env variable")
	}



	Given(`^Some freshly stored events$`, func() {
		if len(configErrors) != 0 {
			assert.Fail(T,strings.Join(configErrors, "\n"))
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
		publisher := orapub.NewOraPub()
		err := publisher.Connect(connectStr)
		assert.Nil(T, err)

		specs, pollErr = publisher.PollEvents()
		assert.Nil(T, pollErr)

		eventPublisher = publisher
	})

	Then(`^The freshly stored events are returned$`, func() {
		var foundIt bool
		var aggCount int
		for _, aid := range specs {
			if aid.AggregateId == aggregateID {
				foundIt = true
				aggCount++
			}
		}

		assert.True(T, foundIt)
		assert.Equal(T, 3, aggCount)
	})

	And(`^the event details can be retrieved$`, func() {
		for i := 1; i <= 3; i++ {
			event, err := eventPublisher.RetrieveEventDetail(aggregateID, i)
			if assert.Nil(T, err) {
				assert.Equal(T, event.Source, aggregateID)
				assert.Equal(T, event.Version, i)
			}
		}

	})

	And(`^published events can be removed from the publish table$`, func() {
		err := eventPublisher.DeleteProcessedEvents(specs)
		assert.Nil(T, err, "Error when deleting processed events")
	})

}
