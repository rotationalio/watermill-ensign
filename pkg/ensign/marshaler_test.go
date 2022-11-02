package ensign_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	pb "github.com/rotationalio/ensign/pkg/api/v1beta1"
	"github.com/rotationalio/watermill-ensign/pkg/ensign"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestEnsignMarshaler(t *testing.T) {
	payload, err := json.Marshal(map[string]interface{}{"color": "red", "age": 42})
	require.NoError(t, err, "could not marshal test payload")

	// Create a message to marshal
	msg := message.NewMessage(uuid.NewString(), payload)
	msg.Metadata = make(message.Metadata)
	msg.Metadata.Set(ensign.MIMEKey, "application/json")
	msg.Metadata.Set(ensign.TypeNameKey, "TestEvent")
	msg.Metadata.Set(ensign.TypeVersionKey, "3")
	msg.Metadata.Set(ensign.EncAlgKey, "AES-GCM")
	msg.Metadata.Set(ensign.EncKeyIDKey, "676c64a5-44f0-4e11-a8a7-52fdb30b4caa")
	msg.Metadata.Set(ensign.CompressAlgKey, "gzip")
	msg.Metadata.Set(ensign.RegionNameKey, "earth")
	msg.Metadata.Set(ensign.PubClientIDKey, "1234abcdef")
	msg.Metadata.Set(ensign.PubIPAddrKey, "127.0.0.1")
	msg.Metadata.Set(ensign.CreatedKey, "2022-11-02T22:21:39.804188Z")

	// Create marshaler
	marshaler := ensign.EventMarshaler{}
	event, err := marshaler.Marshal("testing", msg)
	require.NoError(t, err, "could not marshal watermill message into ensign event")

	// Load the event proto fixture from disk
	fixture, err := loadFixture("testdata/pubevent.pb.json")
	require.NoError(t, err, "could not load ensign event fixture from testdata")
	require.True(t, proto.Equal(event, fixture), "the marshaled event does not match the fixture event")
}

func TestInvalidMarshaler(t *testing.T) {
	marshaler := ensign.EventMarshaler{}

	t.Run("ReservedKey", func(t *testing.T) {
		reserved := []string{
			ensign.IDKey, ensign.TopicIDKey, ensign.CommittedKey,
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
	fixture, err := loadFixture("testdata/subevent.pb.json")
	require.NoError(t, err, "could not load ensign event fixture from testdata")

	// Create Unmarshaler
	marshaler := ensign.EventMarshaler{}
	msg, err := marshaler.Unmarshal(fixture)
	require.NoError(t, err, "could not unmarshal ensign event into watermill message")

	payload, err := json.Marshal(map[string]interface{}{"color": "red", "age": 42})
	require.NoError(t, err, "could not marshal test payload")
	require.True(t, bytes.Equal(msg.Payload, payload), "fixture payload does not match unmarshaled payload")

	requireMeta := func(key, value string) {
		require.Contains(t, msg.Metadata, key, "metadata is missing expected key")
		require.Equal(t, value, msg.Metadata.Get(key), "metadata value doesn't match expected value")
	}

	requireMeta(ensign.IDKey, "0623mvwcbg000044")
	requireMeta(ensign.TopicIDKey, "0623mvwcbg00001a")
	requireMeta(ensign.MIMEKey, "application/json")
	requireMeta(ensign.TypeNameKey, "TestEvent")
	requireMeta(ensign.TypeVersionKey, "3")
	requireMeta(ensign.EncAlgKey, "AES-GCM")
	requireMeta(ensign.EncKeyIDKey, "676c64a5-44f0-4e11-a8a7-52fdb30b4caa")
	requireMeta(ensign.CompressAlgKey, "gzip")
	requireMeta(ensign.RegionNameKey, "earth")
	requireMeta(ensign.PubClientIDKey, "1234abcdef")
	requireMeta(ensign.PubIPAddrKey, "127.0.0.1")
	requireMeta(ensign.CreatedKey, "2022-11-02T22:21:39.804188Z")
}

func TestEmptyEvent(t *testing.T) {
	marshaler := ensign.EventMarshaler{}
	msg := message.NewMessage("", nil)

	event, err := marshaler.Marshal("testing", msg)
	require.NoError(t, err, "should be able to marshal an empty message")

	_, err = marshaler.Unmarshal(event)
	require.NoError(t, err, "should be able to unmarshal an empty event")
}

func loadFixture(path string) (event *pb.Event, err error) {
	var data []byte
	if data, err = os.ReadFile(path); err != nil {
		return nil, err
	}

	pbjson := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}

	event = &pb.Event{}
	if err = pbjson.Unmarshal(data, event); err != nil {
		return nil, err
	}
	return event, nil
}
