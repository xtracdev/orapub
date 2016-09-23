package orapub

import (
	"database/sql"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-oci8"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/oraconn"
)

type EventProcessor struct {
	Initialize func(*sql.DB) error
	Processor  func(db *sql.DB, e *goes.Event) error
}

type EventSpec struct {
	AggregateId string
	Version     int
}

var eventProcessors map[string]EventProcessor

func init() {
	eventProcessors = make(map[string]EventProcessor)
}

type OraPub struct {
	db *oraconn.OracleDB
}

var ErrNilEventProcessorField = errors.New("Registered event processor with one or more nil fields.")

func RegisterEventProcessor(name string, eventProcessor EventProcessor) error {
	if eventProcessor.Processor == nil || eventProcessor.Initialize == nil {
		return ErrNilEventProcessorField
	}
	eventProcessors[name] = eventProcessor

	return nil
}

func (op *OraPub) InitializeProcessors() error {
	for k, p := range eventProcessors {
		log.Infof("Initializing %s", k)
		err := p.Initialize(op.db.DB)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *OraPub) ProcessEvent(event *goes.Event) {
	for _, p := range eventProcessors {
		err := p.Processor(op.db.DB, event)
		if err != nil {
			log.Warnf("Error processing event %v: %s", event, err.Error())
		}
	}
}

//Connect to elcaro - connect string looks like user/password@//host:port/service
func (op *OraPub) Connect(connectStr string, maxTrys int) error {
	db, err := oraconn.OpenAndConnect(connectStr, maxTrys)
	if err != nil {
		log.Warnf("Error connecting to oracle: %s", err.Error())
		return err
	}

	op.db = db

	return nil
}

func (op *OraPub) handleDBError(err error) {
	if oraconn.IsConnectionError(err) {
		op.db.Reconnect(5)
	}
}

func (op *OraPub) PollEvents() ([]EventSpec, error) {
	var eventSpecs []EventSpec

	//Select a batch of events, but no more than 500
	rows, err := op.db.Query(`select aggregate_id, version from publish where rownum < 501 order by version`)
	if err != nil {
		op.handleDBError(err)
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
	if err != nil {
		op.handleDBError(err)
	}

	return eventSpecs, err
}

func (op *OraPub) DeleteProcessedEvents(specs []EventSpec) error {
	for _, es := range specs {
		_, err := op.db.Exec("delete from publish where aggregate_id = :1 and version = :2",
			es.AggregateId, es.Version)
		if err != nil {
			log.Warnf("Error deleting aggregate, version %s, %d: %s", es.AggregateId, es.Version, err.Error())
			op.handleDBError(err)
		}
	}

	return nil
}

func (op *OraPub) RetrieveEventDetail(aggregateId string, version int) (*goes.Event, error) {
	row, err := op.db.Query("select typecode, payload from events where aggregate_id = :1 and version = :2",
		aggregateId, version)
	if err != nil {
		op.handleDBError(err)
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
		op.handleDBError(err)
		return nil, err
	}

	eventPtr := &goes.Event{
		Source:   aggregateId,
		Version:  version,
		TypeCode: typecode,
		Payload:  payload,
	}

	//log.Infof("Event read from db: %v", *eventPtr)

	return eventPtr, nil
}
