package ensign

import (
	"context"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	ensign "github.com/rotationalio/go-ensign"
	api "github.com/rotationalio/go-ensign/api/v1beta1"

	internalSync "github.com/ThreeDotsLabs/watermill/pubsub/sync"
)

type Subscriber struct {
	sync.RWMutex

	config SubscriberConfig
	logger watermill.LoggerAdapter

	client  *ensign.Client
	subs    []handler
	closing chan struct{}
	topicID string

	outputsWg            sync.WaitGroup
	processingMessagesWg sync.WaitGroup
}

var _ message.Subscriber = &Subscriber{}
var _ message.SubscribeInitializer = &Subscriber{}

type SubscriberConfig struct {
	// Ensign config is used to overwrite the Ensign connection configuration
	EnsignConfig *ensign.Options

	// Specify a client directly rather than connecting via the config
	// Cannot specify both an EnsignConfig and a Client
	Client *ensign.Client

	// Unmarshaler is used to convert Ensign events into watermill messages
	Unmarshaler Unmarshaler

	// How long subscriber should wait for Ack/Nack. When no Ack/Nack was received, message will be redelivered.
	AckWaitTimeout time.Duration

	// CloseTimeout determines how long subscriber will wait for Ack/Nack on close.
	// When no Ack/Nack is received after CloseTimeout, subscriber will be closed.
	CloseTimeout time.Duration

	// Create the topic if it doesn't exist when subscribing (default false).
	EnsureCreateTopic bool
}

func (c *SubscriberConfig) setDefaults() {
	if c.Unmarshaler == nil {
		c.Unmarshaler = EventMarshaler{}
	}

	if c.CloseTimeout <= 0 {
		c.CloseTimeout = time.Second * 30
	}

	if c.AckWaitTimeout <= 0 {
		c.AckWaitTimeout = time.Second * 30
	}
}

func (c SubscriberConfig) Validate() error {
	if c.Unmarshaler == nil {
		return ErrMissingUnmarshaler
	}

	if c.EnsignConfig != nil && c.Client != nil {
		return ErrAmbiguousConfig
	}

	return nil
}

// NewSubscriber creates a new Ensign subscriber stream and connects to Ensign.
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (sub *Subscriber, err error) {
	config.setDefaults()
	if err = config.Validate(); err != nil {
		return nil, err
	}

	sub = &Subscriber{
		config:  config,
		client:  config.Client,
		closing: make(chan struct{}),
	}

	if sub.client == nil {
		if sub.client, err = ensign.New(config.EnsignConfig); err != nil {
			return nil, errors.Wrap(err, "could not connect to ensign")
		}
	}

	if sub.logger == nil {
		sub.logger = watermill.NopLogger{}
	}

	return sub, nil
}

// SubscribeInitialize satisfies one of Watermill's interfaces. It is not
// necessary to manually call it. The same initialization performed by this
// function is performed by subscribe.
func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	if topic == "" {
		return ErrEmptyTopic
	}

	// Check if the topic exists
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var exists bool
	if exists, err = s.client.TopicExists(ctx, topic); err != nil {
		return err
	}

	if !exists {
		if s.config.EnsureCreateTopic {
			if s.topicID, err = s.client.CreateTopic(ctx, topic); err != nil {
				return err
			}
			return nil
		}
		return ErrTopicNotFound
	} else {
		if s.topicID, err = s.client.TopicID(ctx, topic); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe to topic.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (_ <-chan *message.Message, err error) {
	if s.topicID == "" {
		// TODO: this shouldn't happen right?
		// TODO: use topic cache rather than fetching topic ID each time.
		if err = s.SubscribeInitialize(topic); err != nil {
			return nil, err
		}
	}

	output := make(chan *message.Message)
	handler := handler{
		sub:  s,
		outc: output,
		logFields: watermill.LogFields{
			"topic": topic,
		},
	}

	if handler.stream, err = s.client.Subscribe(ctx, s.topicID); err != nil {
		return nil, errors.Wrapf(err, "cannot subscribe to topic %q", err)
	}

	var events <-chan *api.Event
	if events, err = handler.stream.Subscribe(); err != nil {
		return nil, errors.Wrapf(err, "cannot subscribe to topic %q", err)
	}

	s.outputsWg.Add(1)
	go handler.run(ctx, events)

	s.Lock()
	s.subs = append(s.subs, handler)
	s.Unlock()

	return output, nil
}

func (s *Subscriber) Close() (err error) {
	s.Lock()
	defer s.Unlock()

	if s.client == nil {
		return nil
	}
	defer func() {
		s.client = nil
		s.subs = nil
	}()

	s.logger.Debug("closing subscriber", nil)
	defer s.logger.Info("ensign subscriber closed", nil)

	close(s.closing)
	internalSync.WaitGroupTimeout(&s.outputsWg, s.config.CloseTimeout)

	if err = s.client.Close(); err != nil {
		return errors.Wrap(err, "could not close ensign client")
	}
	return nil
}

func (s *Subscriber) isClosed() bool {
	s.RLock()
	defer s.RUnlock()
	return s.client == nil
}

type handler struct {
	sub       *Subscriber
	stream    ensign.Subscriber
	outc      chan<- *message.Message
	logFields watermill.LogFields
}

func (s handler) run(ctx context.Context, events <-chan *api.Event) {
msgs:
	for {
		select {
		case <-s.sub.closing:
			break msgs
		case <-ctx.Done():
			break msgs
		case event := <-events:
			s.process(ctx, event)
		}
	}

	if err := s.Close(); err != nil {
		s.sub.logger.Error("could not close subscriber stream", err, s.logFields)
	}

	s.sub.processingMessagesWg.Wait()
	s.sub.outputsWg.Done()
}

func (s handler) process(ctx context.Context, event *api.Event) {
	if s.sub.isClosed() {
		return
	}

	if err := s.stream.Err(); err != nil {
		s.sub.logger.Error("ensign subscription stream errored", err, s.logFields)
		return
	}

	s.sub.processingMessagesWg.Add(1)
	defer s.sub.processingMessagesWg.Done()

	s.sub.logger.Trace("recv event", s.logFields)

	msg, err := s.sub.config.Unmarshaler.Unmarshal(event)
	if err != nil {
		s.sub.logger.Error("cannot unmarshal message", err, s.logFields)
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancel()

	messageFields := s.logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.sub.logger.Trace("unmarshaled message", messageFields)

	select {
	case s.outc <- msg:
		s.sub.logger.Trace("message delivered", messageFields)
	case <-s.sub.closing:
		s.sub.logger.Trace("closing, message discarded", messageFields)
		return
	case <-ctx.Done():
		s.sub.logger.Trace("context canceled, message discarded", messageFields)
		return
	}

	select {
	case <-msg.Acked():
		if err := s.stream.Ack(event.Id); err != nil {
			s.sub.logger.Error("cannot send ack", err, messageFields)
			return
		}
		s.sub.logger.Trace("message acked", messageFields)
		return
	case <-msg.Nacked():
		if err := s.stream.Nack(event.Id, nil); err != nil {
			s.sub.logger.Error("cannot send nack", err, messageFields)
			return
		}
		s.sub.logger.Trace("message nacked", messageFields)
		return
	case <-time.After(s.sub.config.AckWaitTimeout):
		s.sub.logger.Trace("ack timeout", messageFields)
		return
	case <-s.sub.closing:
		s.sub.logger.Trace("closing, message discarded before ack", messageFields)
		return
	case <-ctx.Done():
		s.sub.logger.Trace("context cancelled, message discarded before ack", messageFields)
		return
	}
}

func (s handler) Close() error {
	return s.stream.Close()
}
