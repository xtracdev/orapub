package orapub

import (
	"github.com/xtraclabs/goes"
	log "github.com/Sirupsen/logrus"
)

type EventProcessor func(e *goes.Event) error

type OraPub struct {
	eventProcessors map[string]EventProcessor
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


