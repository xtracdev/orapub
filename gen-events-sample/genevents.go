package main

import (
	"os"
	"github.com/xtraclabs/goessample/testagg"
	"fmt"
	"github.com/xtraclabs/oraeventstore"
	"log"
	"strconv"
)

func main() {

	if len(os.Args) != 2 {
		log.Fatalf("Number of aggregate not specified on command line (go run genevents.go <num aggregates>")
	}

	numAggregates, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err.Error())
	}

	os.Setenv("ES_PUBLISH_EVENTS", "1")

	eventStore, err := oraeventstore.NewPGEventStore("esusr", "password", "xe.oracle.docker", "localhost", "1521")
	if err != nil {
		log.Fatalf("Error connecting to oracle: %s", err.Error())
	}

	for i := 0; i < numAggregates; i++ {

		ta,_ := testagg.NewTestAgg(
			fmt.Sprintf("foo%d",i),
			fmt.Sprintf("bar%d",i),
			fmt.Sprintf("baz%d",i))
		for j := 1; j <= 5; j++ {
			ta.UpdateFoo(fmt.Sprintf("foo%d-%d",i,j))
		}

		err = ta.Store(eventStore)
		if err != nil {
			log.Fatalf("Error storing events: %s", err.Error())
		}
	}
}
