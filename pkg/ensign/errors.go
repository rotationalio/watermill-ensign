package ensign

import "errors"

var (
	ErrEmptyTopic       = errors.New("topic is not specified")
	ErrPublisherClosed  = errors.New("publisher is closed")
	ErrSubscriberClosed = errors.New("subscriber is closed")
	ErrMissingMarshaler = errors.New("invalid config: missing marshaler")
	ErrAmbiguousConfig  = errors.New("invalid config: specify ensign options or client not both")
)
