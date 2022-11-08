package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	api "github.com/rotationalio/ensign/sdks/go"
	"github.com/rotationalio/watermill-ensign/pkg/ensign"
)

var (
	consumeTopic = "events"
	publishTopic = "events-processed"

	logger = watermill.NewStdLogger(
		true, // debug
		true, // trace
	)
	marshaler = ensign.EventMarshaler{}
)

type event struct {
	ID int `json:"id"`
}

type processedEvent struct {
	ProcessedID int       `json:"processed_id"`
	Time        time.Time `json:"time"`
}

func main() {
	publisher := createPublisher()

	// Subscriber is created with consumer group handler
	subscriber := createSubscriber("my_handler")

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	// Adding a handler (multiple handlers can be added)
	router.AddHandler(
		"my_handler", // handler name, must be unique
		consumeTopic, // topic from which messages should be consumed
		subscriber,
		publishTopic, // topic to which messages should be published
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			consumedPayload := event{}
			err := json.Unmarshal(msg.Payload, &consumedPayload)
			if err != nil {
				// When a handler returns an error, the default behavior is to send a Nack (negative-acknowledgement).
				// The message will be processed again.
				//
				// You can change the default behavior by using middlewares, like Retry or PoisonQueue.
				// You can also implement your own middleware.
				return nil, err
			}

			log.Printf("received event %+v", consumedPayload)

			newPayload, err := json.Marshal(processedEvent{
				ProcessedID: consumedPayload.ID,
				Time:        time.Now(),
			})
			if err != nil {
				return nil, err
			}

			newMessage := message.NewMessage(watermill.NewUUID(), newPayload)

			return []*message.Message{newMessage}, nil
		},
	)

	// Simulate incoming events in the background
	go simulateEvents(publisher)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

// createPublisher is a helper function that creates a Publisher, in this case - the Ensign Publisher.
func createPublisher() message.Publisher {
	ensignPublisher, err := ensign.NewPublisher(
		ensign.PublisherConfig{
			Marshaler: marshaler,
			EnsignConfig: &api.Options{
				Endpoint: "localhost:3333",
				Insecure: true,
			},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return ensignPublisher
}

// createSubscriber is a helper function similar to the previous one, but in this case it creates a Subscriber.
func createSubscriber(consumerGroup string) message.Subscriber {
	ensignSubscriber, err := ensign.NewSubscriber(
		ensign.SubscriberConfig{
			Unmarshaler: marshaler,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return ensignSubscriber
}

// simulateEvents produces events that will be later consumed.
func simulateEvents(publisher message.Publisher) {
	i := 0
	for {
		e := event{
			ID: i,
		}

		payload, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}

		err = publisher.Publish(consumeTopic, message.NewMessage(
			watermill.NewUUID(), // internal uuid of the message, useful for debugging
			payload,
		))
		if err != nil {
			panic(err)
		}

		i++

		time.Sleep(time.Second)
	}
}
