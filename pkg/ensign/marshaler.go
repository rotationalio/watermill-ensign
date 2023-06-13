package ensign

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	api "github.com/rotationalio/go-ensign"
	pb "github.com/rotationalio/go-ensign/api/v1beta1"
	mime "github.com/rotationalio/go-ensign/mimetype/v1beta1"
	region "github.com/rotationalio/go-ensign/region/v1beta1"
)

// Header keys that the ensign marshaler expects to find in message metadata in order
// to convert the Waterfall message into an Ensign event.
const (
	// Event Data
	MIMEKey        = "mimetype"
	TypeNameKey    = "type_name"
	TypeVersionKey = "type_version"
	CreatedKey     = "created"

	// Watermill Parity
	WatermillUUIDKey = "watermill_uuid"

	// Event Info
	IDKey                 = "event_id"
	TopicIDKey            = "topic_id"
	OffsetKey             = "offset"
	EpochKey              = "epoch"
	RegionKey             = "region"
	PublisherIDKey        = "publisher_id"
	PublisherIPAddrKey    = "publisher_ipaddr"
	PublisherClientIDKey  = "publisher_client_id"
	PublisherUserAgentKey = "publisher_user_agent"
	KeyKey                = "key"
	ShardKey              = "shard"
	EncAlgKey             = "encryption_algorithm"
	EncKeyIDKey           = "encryption_public_key_id"
	EncSigKey             = "encryption_signature"
	CompressAlgKey        = "compression_algorithm"
	CompressLevelKey      = "compression_level"
	CommittedKey          = "committed"
)

// The number of metadata keys available in Ensign (to alloc metadata)
const nEnsignKeys = 15

// Reserved metadata keys that cannot be in a message for serialization.
var reserved = []string{
	IDKey, TopicIDKey, OffsetKey, EpochKey, WatermillUUIDKey,
	RegionKey, PublisherIDKey, PublisherIPAddrKey,
	EncAlgKey, EncSigKey, ShardKey, CommittedKey,
}

// Marshaler transforms a Waterfall Message into an Ensign client library Event.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*api.Event, error)
}

// Unmarshaler transfers an Ensign client library Event into a Waterfall Message.
type Unmarshaler interface {
	Unmarshal(*api.Event) (*message.Message, error)
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

func (e EventMarshaler) Marshal(topic string, msg *message.Message) (event *api.Event, err error) {
	// Check if any of the reserved keys have been specified in the message metadata
	for _, reservedKey := range reserved {
		if value := msg.Metadata.Get(reservedKey); value != "" {
			return nil, fmt.Errorf("metadata %s is reserved by ensign", reservedKey)
		}
	}

	// TODO: how to add topic ID to the event from the topic string (or validate it)?
	event = &api.Event{
		Data:     msg.Payload,
		Metadata: make(api.Metadata, len(msg.Metadata)),
	}

	for key, val := range msg.Metadata {
		event.Metadata[key] = val
	}

	event.Metadata[WatermillUUIDKey] = msg.UUID

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
			if err = event.Type.ParseSemver(vers); err != nil {
				return nil, fmt.Errorf("could not parse metadata type version: %w", err)
			}
		}
	}

	if value := msg.Metadata.Get(CreatedKey); value != "" {
		var ts time.Time
		if ts, err = time.Parse(time.RFC3339Nano, value); err != nil {
			return nil, fmt.Errorf("could not parse created timestamp: %w", err)
		}
		event.Created = ts
	}

	return event, nil
}

func (e EventMarshaler) Unmarshal(event *api.Event) (*message.Message, error) {
	// Create metadata from ensign event headers
	metadata := make(message.Metadata, nEnsignKeys+len(event.Metadata))
	metadata.Set(IDKey, event.ID())
	metadata.Set(TopicIDKey, event.TopicID())
	metadata.Set(MIMEKey, event.Mimetype.MimeType())

	// Set the event type with semantic version into the message metadata
	if event.Type != nil {
		metadata.Set(TypeNameKey, event.Type.Name)
		metadata.Set(TypeVersionKey, event.Type.Semver())
	}

	info := event.Info()
	if info != nil {

		metadata.Set(OffsetKey, strconv.FormatUint(info.Offset, 10))
		metadata.Set(EpochKey, strconv.FormatUint(info.Epoch, 10))

		if info.Region != region.Region_UNKNOWN {
			metadata.Set(RegionKey, info.Region.String())
		}

		if info.Publisher != nil && info.Publisher.PublisherId != "" {
			metadata.Set(PublisherIDKey, info.Publisher.PublisherId)
			metadata.Set(PublisherIPAddrKey, info.Publisher.Ipaddr)

			if info.Publisher.ClientId != "" {
				metadata.Set(PublisherClientIDKey, info.Publisher.ClientId)
			}

			if info.Publisher.UserAgent != "" {
				metadata.Set(PublisherUserAgentKey, info.Publisher.UserAgent)
			}
		}

		if len(info.Key) > 0 {
			// TODO: should we add base64 encoding to the key?
			metadata.Set(KeyKey, string(info.Key))
			metadata.Set(ShardKey, strconv.FormatUint(info.Shard, 16))
		}

		if info.Encryption != nil {
			metadata.Set(EncAlgKey, info.Encryption.EncryptionAlgorithm.String())
			metadata.Set(EncKeyIDKey, info.Encryption.PublicKeyId)
			metadata.Set(EncSigKey, hex.EncodeToString(info.Encryption.Signature))
		}

		if info.Compression != nil {
			metadata.Set(CompressAlgKey, info.Compression.Algorithm.String())
			if info.Compression.Level > 0 {
				metadata.Set(CompressLevelKey, strconv.FormatInt(info.Compression.Level, 10))
			}
		}
	}

	// Set the created and committed timestamps
	if !event.Created.IsZero() {
		metadata.Set(CreatedKey, event.Created.Format(time.RFC3339Nano))
	}

	if !event.Committed().IsZero() {
		metadata.Set(CommittedKey, event.Committed().Format(time.RFC3339Nano))
	}

	// Set the user metadata back onto the message.
	// NOTE: user keys will override ensign metadata keys that are not reserved.
	for key, val := range event.Metadata {
		metadata.Set(key, val)
	}

	// Attempt to determine the message UUID
	var ok bool
	var uuid string
	if uuid, ok = event.Metadata[WatermillUUIDKey]; !ok {
		if info := event.Info(); info != nil {
			uuid = string(info.LocalId)
		}
	}

	msg := message.NewMessage(uuid, event.Data)
	msg.Metadata = metadata
	delete(msg.Metadata, WatermillUUIDKey)

	return msg, nil
}
