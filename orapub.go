package orapub

import (
	"database/sql"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-oci8"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/oraconn"
	"time"
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

func ClearRegisteredEventProcessors() {
	eventProcessors = make(map[string]EventProcessor)
}

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

func (op *OraPub) PollEvents(tx *sql.Tx) ([]EventSpec, error) {
	var eventSpecs []EventSpec

	if tx == nil {
		var makeTxErr error
		log.Warn("No TX provided to PollEvents - creating tx")
		tx, makeTxErr = op.db.Begin()
		if makeTxErr != nil {
			return nil,makeTxErr
		}
		defer tx.Rollback()
	}

	//Select a batch of events, but no more than 100
	rows, err := tx.Query(`select aggregate_id, version from publish where rownum < 101 order by version for update`)
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

func (op *OraPub) deleteEvent(tx *sql.Tx, es EventSpec) error {
	_, err := tx.Exec("delete from publish where aggregate_id = :1 and version = :2",
		es.AggregateId, es.Version)
	if err != nil {
		log.Warnf("Error deleting aggregate, version %s, %d: %s", es.AggregateId, es.Version, err.Error())
		op.handleDBError(err)
	}

	return err
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

func (op *OraPub) ProcessEvents(loop bool) {
	for {
		log.Debug("start process events transaction")
		txn, err := op.db.Begin()
		if err != nil {
			log.Warn(err.Error())
			txn.Rollback()
			break
		}


		log.Debug("poll for events")
		eventSpecs, err := op.PollEvents(txn)
		if err != nil {
			log.Warn(err.Error())
			txn.Rollback()
			break
		}

		if len(eventSpecs) == 0 {
			log.Infof("Nothing to do... time for a 5 second sleep")
			txn.Rollback()
			time.Sleep(5 * time.Second)
			break
		}

		log.Debug("process events")
		for _, eventContext := range eventSpecs {

			log.Debugf("process %s:%d", eventContext.AggregateId, eventContext.Version)
			e, err := op.RetrieveEventDetail(eventContext.AggregateId, eventContext.Version)
			if err != nil {
				log.Warnf("Error reading event to process (%v): %s", eventContext, err)
				continue
			}

			for _, processor := range eventProcessors {
				log.Debug("call processor")
				procErr := processor.Processor(op.db.DB, e)
				if procErr == nil {
					op.deleteEvent(txn, eventContext)
				} else {
					log.Warnf("error processing event %v: %s", e, procErr.Error())
				}
			}

		}

		log.Debug("commit txn")
		txn.Commit()
		if loop != true {
			break
		}
	}
}