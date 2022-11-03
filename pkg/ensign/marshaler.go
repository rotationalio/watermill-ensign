package ensign

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	pb "github.com/rotationalio/ensign/pkg/api/v1beta1"
	mime "github.com/rotationalio/ensign/pkg/mimetype/v1beta1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Header keys that the ensign marshaler expects to find in message metadata in order
// to convert the Waterfall message into an Ensign event.
const (
	IDKey          = "id"
	TopicIDKey     = "topic_id"
	MIMEKey        = "mimetype"
	TypeNameKey    = "type_name"
	TypeVersionKey = "type_version"
	KeyKey         = "key"
	EncAlgKey      = "encryption_algorithm"
	EncKeyIDKey    = "encryption_key_id"
	CompressAlgKey = "compression_algorithm"
	RegionNameKey  = "region_name"
	PubClientIDKey = "publisher_client_id"
	PubIPAddrKey   = "publisher_ipaddr"
	UUIDKey        = "user_defined_id"
	CreatedKey     = "created"
	CommittedKey   = "committed"
)

// The number of metadata keys available in Ensign (to alloc metadata)
const nEnsignKeys = 15

// Reserved metadata keys that cannot be in a message for serialization.
var reserved = []string{
	IDKey, TopicIDKey, CommittedKey, UUIDKey,
}

// Marshaler transforms a Waterfall Message into an Ensign client library Event.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*pb.Event, error)
}

// Unmarshaler transfers an Ensign client library Event into a Waterfall Message.
type Unmarshaler interface {
	Unmarshal(*pb.Event) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

// EventMarshaler is the default Marshaler and Unmarshaler that converts Waterfall
// messages into Ensign events and vice versa. The message metadata is primarily used
// to populate the Ensign-specific metadata fields and the Payload is simply converted
// into the Event payload without much serialization.
type EventMarshaler struct{}

var _ Marshaler = &EventMarshaler{}
var _ Unmarshaler = &EventMarshaler{}
var _ MarshalerUnmarshaler = &EventMarshaler{}

func (e EventMarshaler) Marshal(topic string, msg *message.Message) (event *pb.Event, err error) {
	// Check if any of the reserved keys have been specified in the message metadata
	for _, reservedKey := range reserved {
		if value := msg.Metadata.Get(reservedKey); value != "" {
			return nil, fmt.Errorf("metadata %s is reserved by ensign", reservedKey)
		}
	}

	// TODO: how to add topic ID to the event from the topic string (or validate it)?
	event = &pb.Event{
		Data:          msg.Payload,
		UserDefinedId: msg.UUID,
	}

	if value := msg.Metadata.Get(KeyKey); value != "" {
		// TODO: should we add base64 encoding/decoding to the key?
		event.Key = []byte(value)
	}

	if value := msg.Metadata.Get(MIMEKey); value != "" {
		if event.Mimetype, err = mime.Parse(value); err != nil {
			return nil, fmt.Errorf("could not parse metadata mimetype: %w", err)
		}
	}

	if value := msg.Metadata.Get(TypeNameKey); value != "" {
		event.Type = &pb.Type{
			Name: value,
		}

		if vers := msg.Metadata.Get(TypeVersionKey); vers != "" {
			var version uint64
			if version, err = strconv.ParseUint(vers, 10, 32); err != nil {
				return nil, fmt.Errorf("could not parse type version: %w", err)
			}
			event.Type.Version = uint32(version)
		}
	}

	if enc := msg.Metadata.Get(EncKeyIDKey); enc != "" {
		event.Encryption = &pb.Encryption{
			Algorithm: msg.Metadata.Get(EncAlgKey),
			KeyId:     enc,
		}
	}

	if cmp := msg.Metadata.Get(CompressAlgKey); cmp != "" {
		event.Compression = &pb.Compression{
			Algorithm: cmp,
		}
	}

	if region := msg.Metadata.Get(RegionNameKey); region != "" {
		event.Geography = &pb.Region{
			Name: region,
		}
	}

	if pub := msg.Metadata.Get(PubClientIDKey); pub != "" {
		event.Publisher = &pb.Publisher{
			ClientId: pub,
			Ipaddr:   msg.Metadata.Get(PubIPAddrKey),
		}
	}

	if value := msg.Metadata.Get(CreatedKey); value != "" {
		var ts time.Time
		if ts, err = time.Parse(time.RFC3339Nano, value); err != nil {
			return nil, fmt.Errorf("could not parse created timestamp: %w", err)
		}
		event.Created = timestamppb.New(ts)
	}

	return event, nil
}

func (e EventMarshaler) Unmarshal(event *pb.Event) (*message.Message, error) {
	// Create metadata from ensign event headers
	metadata := make(message.Metadata, nEnsignKeys)
	metadata.Set(IDKey, event.Id)
	metadata.Set(TopicIDKey, event.TopicId)
	metadata.Set(MIMEKey, event.Mimetype.MimeType())

	if event.Key != nil {
		// TODO: should we add base64 encoding to the key?
		metadata.Set(KeyKey, string(event.Key))
	}

	if event.Type != nil {
		metadata.Set(TypeNameKey, event.Type.Name)
		metadata.Set(TypeVersionKey, strconv.FormatUint(uint64(event.Type.Version), 10))
	}

	if event.Encryption != nil {
		metadata.Set(EncAlgKey, event.Encryption.Algorithm)
		metadata.Set(EncKeyIDKey, event.Encryption.KeyId)
	}

	if event.Compression != nil {
		metadata.Set(CompressAlgKey, event.Compression.Algorithm)
	}

	if event.Geography != nil {
		metadata.Set(RegionNameKey, event.Geography.Name)
	}

	if event.Publisher != nil {
		metadata.Set(PubClientIDKey, event.Publisher.ClientId)
		metadata.Set(PubIPAddrKey, event.Publisher.Ipaddr)
	}

	if event.Created != nil {
		metadata.Set(CreatedKey, event.Created.AsTime().Format(time.RFC3339Nano))
	}

	if event.Committed != nil {
		metadata.Set(CommittedKey, event.Committed.AsTime().Format(time.RFC3339Nano))
	}

	msg := message.NewMessage(event.UserDefinedId, event.Data)
	msg.Metadata = metadata

	return msg, nil
}
