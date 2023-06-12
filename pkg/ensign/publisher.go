package ensign

import (
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	ensign "github.com/rotationalio/go-ensign"
	"github.com/rotationalio/go-ensign/topics"
)

type Publisher struct {
	sync.RWMutex

	config PublisherConfig
	logger watermill.LoggerAdapter

	client *ensign.Client
	topics *topics.Cache
}

var _ message.Publisher = &Publisher{}

type PublisherConfig struct {
	// Ensign config is used to overwrite the Ensign connection configuration
	// Supply Ensign API Key credentials directly in this configuration or set the
	// $ENSIGN_CLIENT_ID and $ENSIGN_CLIENT_SECRET environment variables.
	EnsignConfig *ensign.Options

	// Specify a client directly rather than connecting via the config
	// Cannot specify both an EnsignConfig and a Client
	Client *ensign.Client

	// Marshaler is used to convert messages into Ensign events
	Marshaler Marshaler

	// Create the topic if it doesn't exist when publishing (default false).
	EnsureCreateTopic bool
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = EventMarshaler{}
	}

	if c.EnsignConfig == nil && c.Client == nil {
		// Ignore any validation errors until the Validate() step in the config
		opts, _ := ensign.NewOptions()
		c.EnsignConfig = &opts
	}
}

func (c PublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return ErrMissingMarshaler
	}

	if c.EnsignConfig != nil && c.Client != nil {
		return ErrAmbiguousConfig
	}

	if c.EnsignConfig != nil {
		if c.EnsignConfig.ClientID == "" || c.EnsignConfig.ClientSecret == "" {
			return ErrMissingCredentials
		}

		// Validate other ensign configuration issues
		if err := c.EnsignConfig.Validate(); err != nil {
			return err
		}
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
		if pub.client, err = ensign.New(ensign.WithOptions(*pub.config.EnsignConfig)); err != nil {
			return nil, errors.Wrap(err, "could not connect to ensign")
		}
	}

	pub.topics = topics.NewCache(pub.client)
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

	// Get the topicID from the topics cache
	var topicID string
	if topicID, err = p.TopicID(topic); err != nil {
		return err
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic
	logFields["topicID"] = topicID

	for _, message := range messages {
		logFields["message_uuid"] = message.UUID
		p.logger.Trace("sending message to Ensign", logFields)

		var event *ensign.Event
		if event, err = p.config.Marshaler.Marshal(topic, message); err != nil {
			return errors.Wrapf(err, "cannot marshal messages %s", message.UUID)
		}

		// TODO: wait for ack and log partition and offset (requires SDK update).
		if err = p.client.Publish(topicID, event); err != nil {
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
	}()

	if err = p.client.Close(); err != nil {
		return errors.Wrap(err, "cannot close ensign client")
	}
	return nil
}

func (p *Publisher) TopicID(topic string) (topicID string, err error) {
	if p.config.EnsureCreateTopic {
		return p.topics.Ensure(topic)
	}
	return p.topics.Get(topic)
}
