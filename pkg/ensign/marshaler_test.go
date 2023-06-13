package ensign_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	api "github.com/rotationalio/go-ensign"
	pb "github.com/rotationalio/go-ensign/api/v1beta1"
	"github.com/rotationalio/watermill-ensign/pkg/ensign"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestEnsignMarshaler(t *testing.T) {
	payload, err := json.Marshal(map[string]interface{}{"color": "red", "age": 42})
	require.NoError(t, err, "could not marshal test payload")

	// Create a message to marshal
	msg := message.NewMessage("8e9c2625-c7fd-49fd-8cee-e7b92213b368", payload)
	msg.Metadata = make(message.Metadata)
	msg.Metadata.Set(ensign.MIMEKey, "application/json")
	msg.Metadata.Set(ensign.TypeNameKey, "TestEvent")
	msg.Metadata.Set(ensign.TypeVersionKey, "1.3.0")
	msg.Metadata.Set(ensign.CreatedKey, "2022-11-02T22:21:39.804188Z")
	msg.Metadata.Set("color", "red")
	msg.Metadata.Set("group", "xchange")

	// Create marshaler
	marshaler := ensign.EventMarshaler{}
	event, err := marshaler.Marshal("testing", msg)
	require.NoError(t, err, "could not marshal watermill message into ensign event")

	// Load the event proto fixture from disk
	wrapper, err := loadFixture("testdata/pubwrapper.pb.json", "testdata/event.pb.json")
	require.NoError(t, err, "could not load ensign event fixture from testdata")

	fixture := api.NewOutgoingEvent(wrapper, nil)
	require.Equal(t, fixture.Mimetype, event.Mimetype, "the marshaled mimetype does not match the fixture mimetype")
	require.Equal(t, fixture.Type, event.Type, "the marshaled type does not match the fixture type")
	require.Equal(t, fixture.Data, event.Data, "the marshaled data does not match the fixture data")
	require.Equal(t, fixture.Created, event.Created, "the marshaled created does not match the fixture created")

	// Require that everything in the fixture metadata is in the event metadata
	for key, val := range fixture.Metadata {
		require.Contains(t, event.Metadata, key, "event data does not contain fixture key")
		require.Equal(t, event.Metadata[key], val, "event metadata does not match fixture for key %q", key)
	}
}

func TestInvalidMarshaler(t *testing.T) {
	marshaler := ensign.EventMarshaler{}

	t.Run("ReservedKey", func(t *testing.T) {
		reserved := []string{
			ensign.IDKey, ensign.TopicIDKey, ensign.CommittedKey, ensign.WatermillUUIDKey,
		}

		for _, reskey := range reserved {
			msg := message.NewMessage(uuid.NewString(), nil)
			msg.Metadata = make(message.Metadata)
			msg.Metadata.Set(reskey, "invalid")

			_, err := marshaler.Marshal("testing", msg)
			require.Error(t, err, "expected error when reserved key is in metadata")
		}
	})

	t.Run("BadMIME", func(t *testing.T) {
		msg := message.NewMessage(uuid.NewString(), nil)
		msg.Metadata = make(message.Metadata)
		msg.Metadata.Set(ensign.MIMEKey, "invalid")

		_, err := marshaler.Marshal("testing", msg)
		require.Error(t, err, "expected error when invalid mimetype is in metadata")
	})

	t.Run("BadTypeVersion", func(t *testing.T) {
		msg := message.NewMessage(uuid.NewString(), nil)
		msg.Metadata = make(message.Metadata)
		msg.Metadata.Set(ensign.TypeNameKey, "invalid")
		msg.Metadata.Set(ensign.TypeVersionKey, "invalid")

		_, err := marshaler.Marshal("testing", msg)
		require.Error(t, err, "expected error when invalid type version is in metadata")
	})

	t.Run("BadCreatedTimestamp", func(t *testing.T) {
		msg := message.NewMessage(uuid.NewString(), nil)
		msg.Metadata = make(message.Metadata)
		msg.Metadata.Set(ensign.CreatedKey, "invalid")

		_, err := marshaler.Marshal("testing", msg)
		require.Error(t, err, "expected error when invalid created timestamp is in metadata")
	})

}

func TestEnsignUnmarshaler(t *testing.T) {
	// Load the event proto fixture from disk
	wrapper, err := loadFixture("testdata/subwrapper.pb.json", "testdata/event.pb.json")
	require.NoError(t, err, "could not load ensign event fixture from testdata")

	fixture := api.NewIncomingEvent(wrapper, nil)
	t.Logf("%+v", fixture.Metadata)

	// Create Unmarshaler
	marshaler := ensign.EventMarshaler{}
	msg, err := marshaler.Unmarshal(fixture)
	require.NoError(t, err, "could not unmarshal ensign event into watermill message")

	payload, err := json.Marshal(map[string]interface{}{"color": "red", "age": 42})
	require.NoError(t, err, "could not marshal test payload")
	require.True(t, bytes.Equal(msg.Payload, payload), "fixture payload does not match unmarshaled payload")
	require.Equal(t, "8e9c2625-c7fd-49fd-8cee-e7b92213b368", msg.UUID, "watermill id not stored")
	require.NotContains(t, msg.Metadata, ensign.WatermillUUIDKey, "expected the watermill UUID to be removed from metadata")

	requireMeta := func(key, value string) {
		require.Contains(t, msg.Metadata, key, "metadata is missing expected key")
		require.Equal(t, value, msg.Metadata.Get(key), "metadata value doesn't match expected value")
	}

	requireMeta(ensign.MIMEKey, "application/json")
	requireMeta(ensign.TypeNameKey, "TestEvent")
	requireMeta(ensign.TypeVersionKey, "1.3.0")
	requireMeta(ensign.CreatedKey, "2022-11-02T22:21:39.804188Z")

	requireMeta(ensign.IDKey, "0623mvwcbg000044")
	requireMeta(ensign.TopicIDKey, "01H2RV2HNHTTT29AS7CJ97R5F0")
	requireMeta(ensign.OffsetKey, "421332")
	requireMeta(ensign.EpochKey, "214")
	requireMeta(ensign.RegionKey, "LKE_US_SOUTHEAST_1A")
	requireMeta(ensign.PublisherIDKey, "01H2RVFTTKBY59KNVJZ0A0MJEF")
	requireMeta(ensign.PublisherIPAddrKey, "127.0.0.1")
	requireMeta(ensign.PublisherClientIDKey, "1234abcdef")
	requireMeta(ensign.PublisherUserAgentKey, "PyEnsign v1.0")
	requireMeta(ensign.KeyKey, "partition1")
	requireMeta(ensign.ShardKey, "0")
	requireMeta(ensign.EncAlgKey, "AES256_GCM")
	requireMeta(ensign.EncKeyIDKey, "676c64a5-44f0-4e11-a8a7-52fdb30b4caa")
	requireMeta(ensign.EncSigKey, "b56a32aafb2ca895cfcafc24147dead3")
	requireMeta(ensign.CompressAlgKey, "GZIP")
	requireMeta(ensign.CompressLevelKey, "6")
	requireMeta(ensign.CommittedKey, "2022-11-03T07:08:01.804626Z")

}

func TestEmptyEvent(t *testing.T) {
	marshaler := ensign.EventMarshaler{}
	msg := message.NewMessage("", nil)

	event, err := marshaler.Marshal("testing", msg)
	require.NoError(t, err, "should be able to marshal an empty message")

	_, err = marshaler.Unmarshal(event)
	require.NoError(t, err, "should be able to unmarshal an empty event")
}

func loadFixture(wrapperPath, eventPath string) (_ *pb.EventWrapper, err error) {
	pbjson := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}

	var data []byte
	if data, err = os.ReadFile(wrapperPath); err != nil {
		return nil, err
	}

	wrapper := &pb.EventWrapper{}
	if err = pbjson.Unmarshal(data, wrapper); err != nil {
		return nil, err
	}

	if data, err = os.ReadFile(eventPath); err != nil {
		return nil, err
	}

	event := &pb.Event{}
	if err = pbjson.Unmarshal(data, event); err != nil {
		return nil, err
	}

	if err = wrapper.Wrap(event); err != nil {
		return nil, err
	}

	return wrapper, nil
}
