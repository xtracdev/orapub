package publisher

import (
	. "github.com/lsegal/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtraclabs/goes"
	"github.com/xtraclabs/orapub"
)

func init() {
	var event goes.Event
	var publisher *orapub.OraPub
	var fooCalled bool
	var barCalled bool

	var fooProcessor = func(event *goes.Event) error {
		fooCalled = true
		return nil
	}

	var barProcessor = func(event *goes.Event) error {
		barCalled = true
		return nil
	}

	Given(`^An event to be published$`, func() {
		event = goes.Event{
			Source:   "xxx",
			Version:  123,
			Payload:  "the payload",
			TypeCode: "tc",
		}
	})

	And(`^Some registered event processors$`, func() {
		publisher = orapub.NewOraPub()
		publisher.RegisterEventProcessor("foo", fooProcessor)
		publisher.RegisterEventProcessor("bar", barProcessor)
	})

	When(`^The publisher processes the event$`, func() {
		publisher.ProcessEvent(&event)
	})

	Then(`^All the registered event processors are invoked with the event$`, func() {
		assert.True(T, fooCalled, "Foo was not called")
		assert.True(T, barCalled, "Bar was not called")
	})

}
