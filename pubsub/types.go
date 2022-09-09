package pubsub

import (
	"context"
	core "github.com/libp2p/go-libp2p-core"
)

// PubSubTopic is a pub sub subscription to a topic
type PubSubTopic interface {
	// Publish Posts a new message on a topic
	Publish(ctx context.Context, message []byte) error

	// Peers Lists peers connected to the topic
	Peers(ctx context.Context) ([]core.PeerID, error)

	// WatchPeers subscribes to peers joining or leaving the topic
	WatchPeers(ctx context.Context) (<-chan interface{}, error)

	// WatchMessages Subscribes to new messages
	WatchMessages(ctx context.Context) (<-chan *EventPubSubMessage, error)

	// Topic Returns the topic name
	Topic() string
}

type PubSubInterface interface {
	// TopicSubscribe Subscribes to a topic
	TopicSubscribe(ctx context.Context, topic string) (PubSubTopic, error)
}

// EventPubSubMessage Indicates a new message posted on a pubsub topic
type EventPubSubMessage struct {
	Content []byte
	From    core.PeerID
}

// EventPubSubPayload An event received on new messages
type EventPubSubPayload struct {
	Payload []byte
	Peer    core.PeerID
}

// EventPubSubJoin Is an event triggered when a peer joins the channel
type EventPubSubJoin struct {
	Topic string
	Peer  core.PeerID
}

// EventPubSubLeave Is an event triggered when a peer leave the channel
type EventPubSubLeave struct {
	Topic string
	Peer  core.PeerID
}

// Creates a new Message event
func NewEventMessage(from core.PeerID, content []byte) *EventPubSubMessage {
	return &EventPubSubMessage{
		From:    from,
		Content: content,
	}
}

// NewEventPayload Creates a new Message event
func NewEventPayload(payload []byte, peerid core.PeerID) *EventPubSubPayload {
	return &EventPubSubPayload{
		Payload: payload,
		Peer:    peerid,
	}
}

// NewEventPeerJoin creates a new EventPubSubJoin event
func NewEventPeerJoin(p core.PeerID, topic string) interface{} {
	return &EventPubSubJoin{
		Peer:  p,
		Topic: topic,
	}
}

// NewEventPeerLeave creates a new EventPubSubLeave event
func NewEventPeerLeave(p core.PeerID, topic string) interface{} {
	return &EventPubSubLeave{
		Peer:  p,
		Topic: topic,
	}
}
