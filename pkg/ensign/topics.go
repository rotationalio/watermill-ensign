package ensign

import (
	"context"
	"time"
)

const DefaultTimeout = 15 * time.Second

// TopicCache manages topics on behalf of the user, looking up topicIDs by name and
// cacheing them to prevent multiple remote requests. The cache should also wrap an
// Ensign client but the cache uses the topic management functionality of the client, so
// an independent interface is added to make testing simpler.
type TopicCache struct {
	topics map[string]string
	client TopicClient
}

type TopicClient interface {
	TopicExists(context.Context, string) (bool, error)
	TopicID(context.Context, string) (string, error)
	CreateTopic(context.Context, string) (string, error)
}

func NewTopicCache(client TopicClient) *TopicCache {
	return &TopicCache{
		topics: make(map[string]string),
		client: client,
	}
}

// Get returns a topicID from a topic; if the topic is not in the cache; an RPC call to
// ensign is made to get and store the topic ID.
func (t *TopicCache) Get(topic string) (topicID string, err error) {
	var cached bool
	if topicID, cached = t.topics[topic]; !cached {
		// Fetch the topicID from Ensign
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		defer cancel()

		if topicID, err = t.client.TopicID(ctx, topic); err != nil {
			return "", err
		}

		// Cache the topicID to prevent future RPC calls
		t.topics[topic] = topicID
	}
	return topicID, nil
}

// Exists checks if the topic exists, first by checking the cache and if the topic is
// not in the cache by performing an RPC call to ensign to check if the topic exists.
func (t *TopicCache) Exists(topic string) (exists bool, err error) {
	// Check if the topic is in the topic cache.
	if _, exists = t.topics[topic]; exists {
		return true, nil
	}

	// Otherwise make a request to Ensign to see if the topic exists
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	return t.client.TopicExists(ctx, topic)
}

// Ensure the topic exists by first performing a check if the topic exists and if it
// doesn't, then creating the topic. The topicID of the created topic is cached to
// prevent repeated calls to CreateTopic that will fail after the first call (topic
// already exists error).
func (t *TopicCache) Ensure(topic string) (topicID string, err error) {
	var cached bool
	if topicID, cached = t.topics[topic]; !cached {
		// Fetch the topicID from Ensign
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		defer cancel()

		// TODO: this could probably be optimized using a call to TopicID and checking
		// if the error is NotFound. The exists check is written here for clarity.
		var exists bool
		if exists, err = t.client.TopicExists(ctx, topic); err != nil {
			return "", err
		}

		if !exists {
			// NOTE: there is a race condition between the existence check and the
			// create topic call (e.g. some other process could create the topic), which
			// would result in an error being returned by CreateTopic. Better error
			// handling would fix this case, since the user only needs the topic to be
			// created and the topicID returned.
			if topicID, err = t.client.CreateTopic(ctx, topic); err != nil {
				// TODO: check ErrTopicAlreadyExists and return no error in this case.
				return "", err
			}
		} else {
			if topicID, err = t.client.TopicID(ctx, topic); err != nil {
				return "", err
			}
		}

		// Cache the topicID to prevent future RPC calls
		t.topics[topic] = topicID
	}
	return topicID, nil
}

// Clear the topic cache resetting any internal cached state and refetching topic info.
func (t *TopicCache) Clear() {
	for key := range t.topics {
		delete(t.topics, key)
	}
}

// Length returns the number of items in the cache
func (t *TopicCache) Length() int {
	return len(t.topics)
}