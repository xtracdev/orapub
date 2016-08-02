package pubreader

import (
	. "github.com/lsegal/gucumber"
	"github.com/xtraclabs/goessample/testagg"
	"fmt"
	"github.com/xtraclabs/oraeventstore"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/xtraclabs/orapub"
	"os"
)

func init() {
	var aggregateID string
	var specs []orapub.EventSpec
	var pollErr error

	Given(`^Some freshly stored events$`, func() {
		os.Setenv("ES_PUBLISH_EVENTS", "1")

		ta,_ := testagg.NewTestAgg("f","b","b")
		ta.UpdateFoo("some new foo")
		ta.UpdateFoo("i changed my mind")
		aggregateID = ta.ID

		eventStore, err := oraeventstore.NewPGEventStore("esusr", "password", "xe.oracle.docker", "localhost", "1521")
		if err != nil {
			log.Infof("Error connecting to oracle: %s", err.Error())
		}
		assert.NotNil(T, eventStore)
		assert.Nil(T, err)

		err = ta.Store(eventStore)
		assert.Nil(T,err)
	})

	When(`^The publish table is polled for events$`, func() {
		var connectStr = fmt.Sprintf("%s/%s@//%s:%s/%s", "esusr", "password", "localhost", "1521", "xe.oracle.docker")
		publisher := orapub.NewOraPub()
		err := publisher.Connect(connectStr)
		assert.Nil(T,err)

		specs, pollErr = publisher.PollEvents()
		assert.Nil(T,pollErr)
	})

	Then(`^The freshly stored events are returned$`, func() {
		var foundIt bool
		var aggCount int
		for _,aid := range specs {
			if aid.AggregateId == aggregateID {
				foundIt = true
				aggCount++
			}
		}

		assert.True(T, foundIt)
		assert.Equal(T, 3, aggCount)
	})

}

