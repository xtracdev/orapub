package orapub

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-oci8"
	"github.com/xtraclabs/goes"
)

type EventProcessor func(e *goes.Event) error

type EventSpec struct {
	AggregateId string
	Version     int
}

type OraPub struct {
	eventProcessors map[string]EventProcessor
	db              *sql.DB
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

func (op *OraPub) PollEvents() ([]EventSpec, error) {
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
			AggregateId: aggID,
			Version:     version,
		}

		eventSpecs = append(eventSpecs, es)
	}

	err = rows.Err()

	return eventSpecs, err
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

func (op *OraPub) RetrieveEventDetail(aggregateId string, version int) (*goes.Event, error) {
	row, err := op.db.Query("select typecode, payload from events where aggregate_id = :1 and version = :2",
		aggregateId, version)
	if err != nil {
		return nil, err
	}

	defer row.Close()

	var typecode string
	var payload []byte
	var scanned bool

	if row.Next() {
		row.Scan(&typecode, &payload)
		scanned = true
	}

	if !scanned {
		return nil, fmt.Errorf("Aggregate %s version %d not found", aggregateId, version)
	}

	err = row.Err()
	if err != nil {
		return nil, err
	}
	
	eventPtr := &goes.Event{
		Source:   aggregateId,
		Version:  version,
		TypeCode: typecode,
		Payload:  payload,
	}

	log.Infof("Event read from db: %v", *eventPtr)

	return eventPtr, nil
}
