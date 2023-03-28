package ensign_test

import (
	"testing"

	"github.com/rotationalio/go-ensign/mock"
	"github.com/rotationalio/watermill-ensign/pkg/ensign"
	"github.com/stretchr/testify/suite"
)

type topicTestSuite struct {
	suite.Suite
	mock  *mock.Ensign
	cache *ensign.TopicCache
}

func (s *topicTestSuite) Before() {
	// Create a new mock ensign server for each test (effectively resetting it)
	s.mock = mock.New(nil)

	// TODO: create an sdk client that can be used as the topic client
	s.cache = ensign.NewTopicCache(nil)
}

func (s *topicTestSuite) After() {
	s.mock = nil
	s.cache = nil
}

func TestTopicsSuite(t *testing.T) {
	suite.Run(t, &topicTestSuite{})
}
