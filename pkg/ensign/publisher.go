package ensign

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	pb "github.com/rotationalio/ensign/pkg/api/v1beta1"
	api "github.com/rotationalio/ensign/sdks/go"
)

type Publisher struct {
	client *api.Client
	stream api.Publisher
}

func (p Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.stream == nil {
		if p.stream, err = p.client.Publish(context.Background()); err != nil {
			return err
		}
	}

	for _, message := range messages {
		event := &pb.Event{
			Id:   message.UUID,
			Data: message.Payload,
		}
		p.stream.Publish(event)
	}
	return nil
}

func (p Publisher) Close() error {
	return p.stream.Close()
}
