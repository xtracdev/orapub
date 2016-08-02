package orapub

import (
	"github.com/xtraclabs/goes"
	log "github.com/Sirupsen/logrus"
	"database/sql"
	_ "github.com/mattn/go-oci8"
)

type EventProcessor func(e *goes.Event) error

type EventSpec struct {
	AggregateId string
	Version int
}

type OraPub struct {
	eventProcessors map[string]EventProcessor
	db      *sql.DB
}


func NewOraPub() *OraPub {
	return &OraPub{
		eventProcessors: make(map[string]EventProcessor),
	}
}

func (op *OraPub) RegisterEventProcessor(name string, eventProcessor EventProcessor) {
	op.eventProcessors[name] = eventProcessor
}

func (op *OraPub) ProcessEvent(event *goes.Event) {
	for _, p := range op.eventProcessors {
		err := p(event)
		if err != nil {
			log.Warnf("Error processing event %v: %s", event, err.Error())
		}
	}
}

//Connect to elcaro - connect string looks like user/password@//host:port/service
func (op *OraPub) Connect(connectStr string) error {
	db, err := sql.Open("oci8", connectStr)
	if err != nil {
		log.Warnf("Error connecting to oracle: %s", err.Error())
		return err
	}

	//Are we really in an ok state for starters?
	err = db.Ping()
	if err != nil {
		log.Infof("Error connecting to oracle: %s", err.Error())
		return err
	}

	op.db = db

	return nil
}

func (op *OraPub) PollEvents()([]EventSpec, error) {
	var eventSpecs []EventSpec

	rows, err := op.db.Query(`select aggregate_id, version from publish order by version`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var version int
	var aggID string

	for rows.Next() {
		rows.Scan(&aggID, &version)
		es := EventSpec{
			AggregateId:aggID,
			Version:version,
		}

		eventSpecs = append(eventSpecs, es)
	}

	err = rows.Err()

	return eventSpecs,err
}

func (op *OraPub) DeleteProcessedEvents(specs []EventSpec) error {
	for _, es := range specs {
		_, err := op.db.Query("delete from publish where aggregate_id = :1 and version = :2",
			es.AggregateId, es.Version)
		if err != nil {
			log.Warn("Error deleting aggregate, version %s, %d: %s", es.AggregateId, es.Version, err.Error())
		}
	}

	return nil
}

