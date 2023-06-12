package ensign

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	api "github.com/rotationalio/go-ensign"
	pb "github.com/rotationalio/go-ensign/api/v1beta1"
	mime "github.com/rotationalio/go-ensign/mimetype/v1beta1"
)

// Header keys that the ensign marshaler expects to find in message metadata in order
// to convert the Waterfall message into an Ensign event.
const (
	IDKey            = "event_id"
	TopicIDKey       = "topic_id"
	MIMEKey          = "mimetype"
	TypeNameKey      = "type_name"
	TypeVersionKey   = "type_version"
	KeyKey           = "key"
	EncAlgKey        = "encryption_algorithm"
	EncKeyIDKey      = "encryption_key_id"
	CompressAlgKey   = "compression_algorithm"
	RegionNameKey    = "region_name"
	PubClientIDKey   = "publisher_client_id"
	PubIPAddrKey     = "publisher_ipaddr"
	WatermillUUIDKey = "watermill_uuiid"
	CreatedKey       = "created"
	CommittedKey     = "committed"
)

// The number of metadata keys available in Ensign (to alloc metadata)
const nEnsignKeys = 15

// Reserved metadata keys that cannot be in a message for serialization.
var reserved = []string{
	IDKey, TopicIDKey, CommittedKey, WatermillUUIDKey,
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
		Metadata: api.Metadata(msg.Metadata),
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
			var version uint64
			if version, err = strconv.ParseUint(vers, 10, 32); err != nil {
				return nil, fmt.Errorf("could not parse type version: %w", err)
			}
			event.Type.MajorVersion = uint32(version)
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
	for key, val := range event.Metadata {
		metadata.Set(key, val)
	}

	// Set the mimetype into the message metadata
	metadata.Set(MIMEKey, event.Mimetype.MimeType())

	// Set the event type with semantic version into the message metadata
	if event.Type != nil {
		metadata.Set(TypeNameKey, event.Type.Name)
		metadata.Set(TypeVersionKey, fmt.Sprintf("%d.%d.%d", event.Type.MajorVersion, event.Type.MinorVersion, event.Type.PatchVersion))
	}

	// Set the created and committed timestamps
	if !event.Created.IsZero() {
		metadata.Set(CreatedKey, event.Created.Format(time.RFC3339Nano))
	}

	if !event.Committed().IsZero() {
		metadata.Set(CommittedKey, event.Committed().Format(time.RFC3339Nano))
	}

	// Attempt to determine the messasge UUID
	var ok bool
	var uuid string
	if uuid, ok = event.Metadata[WatermillUUIDKey]; !ok {
		if info := event.Info(); info != nil {
			uuid = string(info.LocalId)
		}
	}

	msg := message.NewMessage(uuid, event.Data)
	msg.Metadata = metadata
	return msg, nil
}
