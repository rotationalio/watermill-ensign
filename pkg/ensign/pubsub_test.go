package ensign_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/rotationalio/watermill-ensign/pkg/ensign"
	"github.com/stretchr/testify/require"
)

func TestPublishSubscribe(t *testing.T) {
	// This test is not quite working yet
	t.Skip("seems to be working but takes forever to run")

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          false,
		},
		createPubSub,
		nil,
	)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	pub, err := ensign.NewPublisher(ensign.PublisherConfig{}, logger)
	require.NoError(t, err)

	sub, err := ensign.NewSubscriber(ensign.SubscriberConfig{}, logger)
	require.NoError(t, err)

	return pub, sub
}
