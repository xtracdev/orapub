package orapub

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/goes/sample/testagg"
	"github.com/xtracdev/oraeventstore"
	"github.com/xtracdev/orapub"
	"os"
	"sync"
)

func init() {
	var pub1, pub2 *orapub.OraPub
	var pubCount = 0
	var eventStore *oraeventstore.OraEventStore
	var esinitError error
	var wg sync.WaitGroup

	orapub.ClearRegisteredEventProcessors()

	var fooProcessor = orapub.EventProcessor{
		Initialize: func(db *sql.DB) error {
			log.Info("initialize called")
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			log.Info("processor called")
			pubCount += 1
			return nil
		},
	}

	Given(`^An event is published$`, func() {
		os.Setenv(oraeventstore.EventPublishEnvVar, "1")

		eventStore, esinitError = oraeventstore.NewOraEventStore(user, password, dbSvc, dbhost, dbPort)
		if esinitError != nil {
			assert.Fail(T, "Error registering fooProcessor")
			return
		}

		testAgg2, err := testagg.NewTestAgg("new foo", "new bar", "new baz")
		assert.Nil(T, err)
		assert.NotNil(T, testAgg2)

		testAgg2.Store(eventStore)

	})

	And(`^there are two publisher instances$`, func() {
		var connectStr = fmt.Sprintf("%s/%s@//%s:%s/%s", user, password, dbhost, dbPort, dbSvc)
		log.Warn(connectStr)

		pub1 = new(orapub.OraPub)
		if conErr := pub1.Connect(connectStr, 5); conErr != nil {
			log.Warn(conErr.Error())
			return
		}

		pub2 = new(orapub.OraPub)
		if conErr := pub2.Connect(connectStr, 5); conErr != nil {
			log.Warn(conErr.Error())
			return
		}

		if err := orapub.RegisterEventProcessor("foo", fooProcessor); err != nil {
			assert.Fail(T, "Error registering fooProcessor")
			return
		}

		pub1.InitializeProcessors()
		pub2.InitializeProcessors()
	})

	When(`^The event is published$`, func() {
		wg.Add(2)
		go func() {
			pub1.ProcessEvents(false)
			wg.Done()
		}()

		go func() {
			pub2.ProcessEvents(false)
			wg.Done()
		}()

		wg.Wait()
	})

	Then(`^The event is processed once$`, func() {
		assert.Equal(T, 1, pubCount)
	})

}
