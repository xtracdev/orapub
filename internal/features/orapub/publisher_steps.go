package orapub

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
	"strings"
)

func init() {
	var event goes.Event
	var publisher *orapub.OraPub
	var fooCalled bool
	var barCalled bool
	var fooInitialized, barInitialized bool

	var fooProcessor = orapub.EventProcessor{
		Initialize: func(db *sql.DB) error {
			fooInitialized = true
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			fooCalled = true
			return nil
		},
	}

	var barProcessor = orapub.EventProcessor{
		Initialize: func(db *sql.DB) error {
			barInitialized = true
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			barCalled = true
			return nil
		},
	}

	Given(`^An event to be published$`, func() {
		if len(configErrors) != 0 {
			assert.Fail(T, strings.Join(configErrors, "\n"))
			return
		}

		event = goes.Event{
			Source:   "xxx",
			Version:  123,
			Payload:  "the payload",
			TypeCode: "tc",
		}
	})

	And(`^Some registered event processors$`, func() {
		var connectStr = fmt.Sprintf("%s/%s@//%s:%s/%s", user, password, dbhost, dbPort, dbSvc)
		log.Warn(connectStr)
		publisher = new(orapub.OraPub)
		err := publisher.Connect(connectStr, 5)
		assert.Nil(T, err)

		if err := orapub.RegisterEventProcessor("foo", fooProcessor); err != nil {
			assert.Fail(T, "Error registering fooProcessor")
			return
		}

		if err := orapub.RegisterEventProcessor("bar", barProcessor); err != nil {
			assert.Fail(T, "Error registering barProcessor")
			return
		}

		publisher.InitializeProcessors()
	})

	When(`^The publisher processes the event$`, func() {
		publisher.ProcessEvent(&event)
	})

	Then(`^All the registered event processors are invoked with the event$`, func() {
		assert.True(T, fooCalled, "Foo was not called")
		assert.True(T, barCalled, "Bar was not called")
		assert.True(T, fooInitialized, "Foo was not initialized")
		assert.True(T, barInitialized, "Bar was not initialized")
	})

}
