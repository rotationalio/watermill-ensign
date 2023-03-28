package ensign_test

import (
	"testing"

	sdk "github.com/rotationalio/go-ensign"
	"github.com/rotationalio/go-ensign/mock"
	"github.com/rotationalio/watermill-ensign/pkg/ensign"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
)

type topicTestSuite struct {
	suite.Suite
	mock  *mock.Ensign
	cache *ensign.TopicCache
}

func (s *topicTestSuite) SetupSuite() {
	assert := s.Assert()

	// Create a new mock ensign server for each test (effectively resetting it)
	s.mock = mock.New(nil)

	// Create an sdk client that can be used as the topic client
	client := &sdk.Client{}
	err := client.ConnectMock(s.mock)
	assert.NoError(err, "could not connect ensign client to mock")

	// Create the cache for testing
	s.cache = ensign.NewTopicCache(client)
}

func (s *topicTestSuite) AfterTest(suiteName, testName string) {
	// Cleanup the mock and the cache after testing.
	s.mock.Reset()
	s.cache.Clear()
}

func TestTopicsSuite(t *testing.T) {
	suite.Run(t, &topicTestSuite{})
}

func (s *topicTestSuite) TestGet() {
	// The topic cache should be empty to start and make a request to Ensign; after
	// which point the topic name should be retrieved from the cache without an RPC.
	require := s.Require()
	require.Equal(0, s.cache.Length(), "expected cache to be empty")

	// Have list topics return a list of topic names to search for the topicID
	err := s.mock.UseFixture(mock.TopicNamesRPC, "testdata/topicnames.pb.json")
	require.NoError(err, "could not load topic names fixture")

	// The first lookup should make a request to the ensign mock
	// Subsequent lookups should simply use the cache
	for i := 0; i < 10; i++ {
		topicID, err := s.cache.Get("testing.topics.topicb")
		require.NoError(err, "could not lookup topic id")
		require.Equal("01GWM936SNSN36JKTMSF9Q3N8B", topicID, "unexpected topicId returned")
	}

	require.Equal(1, s.cache.Length(), "expected cache to only have one item")
	require.Equal(1, s.mock.Calls[mock.TopicNamesRPC], "expected the RPC to be called only once")
	require.Len(s.mock.Calls, 1, "expected only one RPC called")

	// A lookup to another topic should not cause the cache to fail
	for i := 0; i < 10; i++ {
		topicID, err := s.cache.Get("testing.topics.topica")
		require.NoError(err, "could not lookup topic id")
		require.Equal("01GWM89049D49FHJH81BT8795H", topicID, "unexpected topicId returned")
	}

	topicID, err := s.cache.Get("testing.topics.topicb")
	require.NoError(err, "could not lookup topic id")
	require.Equal("01GWM936SNSN36JKTMSF9Q3N8B", topicID, "unexpected topicId returned")

	require.Equal(2, s.cache.Length(), "expected cache to only have one item")
	require.Equal(2, s.mock.Calls[mock.TopicNamesRPC], "expected the RPC to be called only once")
	require.Len(s.mock.Calls, 1, "expected only one RPC called")
}

func (s *topicTestSuite) TestGetFail() {
	// Test errors returned from topic Get
	require := s.Require()
	require.Equal(0, s.cache.Length(), "expected cache to be empty")

	// Test error returned from server
	s.mock.UseError(mock.TopicNamesRPC, codes.DataLoss, "something bad happened")
	_, err := s.cache.Get("testing.topics.topicc")
	require.EqualError(err, "rpc error: code = DataLoss desc = something bad happened", "expected the error from the ensign server")

	// Test not found in fixtures
	err = s.mock.UseFixture(mock.TopicNamesRPC, "testdata/topicnames.pb.json")
	require.NoError(err, "could not load topic names fixture")

	_, err = s.cache.Get("testing.topics.does-not-exist")
	require.ErrorIs(err, sdk.ErrTopicNameNotFound)

	require.Equal(0, s.cache.Length(), "expected cache to be empty")
	require.Equal(2, s.mock.Calls[mock.TopicNamesRPC], "expected the RPC to be called only once")
	require.Len(s.mock.Calls, 1, "expected only one RPC called")
}
