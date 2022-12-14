package ensign

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	api "github.com/rotationalio/ensign/pkg/api/v1beta1"
	ensign "github.com/rotationalio/ensign/sdks/go"
)

type Publisher struct {
	sync.RWMutex

	config PublisherConfig
	logger watermill.LoggerAdapter

	client *ensign.Client
	stream ensign.Publisher
}

var _ message.Publisher = &Publisher{}

type PublisherConfig struct {
	// Ensign config is used to overwrite the Ensign connection configuration
	EnsignConfig *ensign.Options

	// Specify a client directly rather than connecting via the config
	// Cannot specify both an EnsignConfig and a Client
	Client *ensign.Client

	// Marshaler is used to convert messages into Ensign events
	Marshaler Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = EventMarshaler{}
	}
}

func (c PublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return ErrMissingMarshaler
	}

	if c.EnsignConfig != nil && c.Client != nil {
		return ErrAmbiguousConfig
	}

	return nil
}

// NewPublisher creates a new Ensign publisher and connects to Ensign.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (pub *Publisher, err error) {
	config.setDefaults()
	if err = config.Validate(); err != nil {
		return nil, err
	}

	pub = &Publisher{
		config: config,
		client: config.Client,
	}

	if pub.client == nil {
		if pub.client, err = ensign.New(config.EnsignConfig); err != nil {
			return nil, errors.Wrap(err, "could not connect to ensign")
		}
	}

	if pub.stream, err = pub.client.Publish(context.Background()); err != nil {
		return nil, errors.Wrap(err, "cannot connect to topic stream")
	}

	if pub.logger == nil {
		pub.logger = watermill.NopLogger{}
	}

	return pub, nil
}

// Publish messsages to Ensign.
// TODO: publisher should not return an until an ack has been received from Ensign.
func (p *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	p.RLock()
	defer p.RUnlock()

	if p.client == nil {
		return ErrPublisherClosed
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, message := range messages {
		logFields["message_uuid"] = message.UUID
		p.logger.Trace("sending message to Ensign", logFields)

		var event *api.Event
		if event, err = p.config.Marshaler.Marshal(topic, message); err != nil {
			return errors.Wrapf(err, "cannot marshal messages %s", message.UUID)
		}

		// TODO: wait for ack and log partition and offset (requires SDK update).
		p.stream.Publish(event)

		// NOTE: errors are not synchronous, e.g. this might not be the error for the
		// currently sent message, it might be an error from a previous message that
		// was sent. We have to change the Ensign SDK in order to get a sync error.
		if err = p.stream.Err(); err != nil {
			return err
		}

		p.logger.Trace("message sent to Ensign", logFields)
	}
	return nil
}

func (p *Publisher) Close() (err error) {
	p.Lock()
	defer p.Unlock()

	if p.client == nil {
		return nil
	}

	defer func() {
		p.client = nil
		p.stream = nil
	}()

	if err = p.stream.Close(); err != nil {
		return errors.Wrap(err, "cannot close ensign stream")
	}

	if err = p.client.Close(); err != nil {
		return errors.Wrap(err, "cannot close ensign client")
	}
	return nil
}
