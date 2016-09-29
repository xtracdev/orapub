package orapub

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/goes"
	"testing"
)

var fooInitialized, barInitialized bool
var fooCalled, barCalled bool

var fooProcessor = EventProcessor{
	Initialize: func(db *sql.DB) error {
		fooInitialized = true
		return nil
	},
	Processor: func(db *sql.DB, event *goes.Event) error {
		fooCalled = true
		return nil
	},
}

var barProcessor = EventProcessor{
	Initialize: func(db *sql.DB) error {
		barInitialized = true
		return nil
	},
	Processor: func(db *sql.DB, event *goes.Event) error {
		barCalled = true
		return nil
	},
}

func initTestState(t *testing.T) bool {
	ClearRegisteredEventProcessors()
	fooInitialized = false
	barInitialized = false
	fooCalled = false
	barCalled = false

	if err := RegisterEventProcessor("foo", fooProcessor); err != nil {
		assert.Fail(t, "Error registering fooProcessor")
		return false
	}

	if err := RegisterEventProcessor("bar", barProcessor); err != nil {
		assert.Fail(t, "Error registering barProcessor")
		return false
	}

	return true
}

func TestAllRegisteredInitialized(t *testing.T) {
	if initTestState(t) {
		pub := new(OraPub)
		pub.InitializeProcessors()
		assert.True(t, fooInitialized, "foo not initialized")
		assert.True(t, barInitialized, "bar not initialized")
	}
}

func TestAllRegisterProcessEvent(t *testing.T) {
	if initTestState(t) {
		pub := new(OraPub)
		pub.processEvent(nil)
		assert.True(t, fooCalled, "foo not called")
		assert.True(t, barCalled, "bar not called")
	}
}
