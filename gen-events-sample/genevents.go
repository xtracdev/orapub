package main

import (
	"fmt"
	"github.com/xtraclabs/goessample/testagg"
	"github.com/xtraclabs/oraeventstore"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatalf("Usage: go run genevents.go <num aggregates> <delay ms>")
	}

	numAggregates, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err.Error())
	}

	delay, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err.Error())
	}

	os.Setenv("ES_PUBLISH_EVENTS", "1")

	eventStore, err := oraeventstore.NewOraEventStore("esusr", "password", "xe.oracle.docker", "localhost", "1521")
	if err != nil {
		log.Fatalf("Error connecting to oracle: %s", err.Error())
	}

	for i := 0; i < numAggregates; i++ {

		ta, _ := testagg.NewTestAgg(
			fmt.Sprintf("foo%d", i),
			fmt.Sprintf("bar%d", i),
			fmt.Sprintf("baz%d", i))
		for j := 1; j <= 5; j++ {
			ta.UpdateFoo(fmt.Sprintf("foo%d-%d", i, j))
		}

		err = ta.Store(eventStore)
		if err != nil {
			log.Fatalf("Error storing events: %s", err.Error())
		}

		log.Println("sleep")
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
}
