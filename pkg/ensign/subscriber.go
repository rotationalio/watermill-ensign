package ensign

import (
	"context"
	"errors"

	"github.com/ThreeDotsLabs/watermill/message"
	api "github.com/rotationalio/ensign/sdks/go"
)

type Subscriber struct {
	client *api.Client
	stream api.Subscriber
}

var _ message.Subscriber = &Subscriber{}
var _ message.SubscribeInitializer = &Subscriber{}

var (
	ErrEmptyTopic = errors.New("topic is not specified")
)

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (_ <-chan *message.Message, err error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	if s.stream == nil {
		if s.stream, err = s.client.Subscribe(ctx); err != nil {
			return nil, err
		}
	}

	msgs := make(chan *message.Message)
	return msgs, nil
}

func (s *Subscriber) Close() error {
	return s.stream.Close()
}

// SubscribeInitialize satisfies one of Watermill's interfaces. It is not
// necessary to manually call it. The same initialization performed by this
// function is performed by subscribe.
func (s *Subscriber) SubscribeInitialize(topic string) error {
	return nil
}
