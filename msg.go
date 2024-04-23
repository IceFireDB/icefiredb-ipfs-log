package icefiredb_ipfs_log

import (
	"encoding/json"

	"berty.tech/go-ipfs-log/entry"
	core "github.com/libp2p/go-libp2p/core"
)

type LocalWrite struct {
	Heads []*entry.Entry
}

func NewLocalWrite(heads []*entry.Entry) LocalWrite {
	return LocalWrite{
		heads,
	}
}

func (m *LocalWrite) Marshal() []byte {
	b, _ := json.Marshal(m)
	return b
}

type RemoteWrite struct {
	From  core.PeerID
	Heads []*entry.Entry
}

func NewRemoteWrite(from core.PeerID, heads []*entry.Entry) RemoteWrite {
	return RemoteWrite{
		from,
		heads,
	}
}

func (m *RemoteWrite) Marshal() []byte {
	b, _ := json.Marshal(m)
	return b
}

type DataLogEvent struct {
	Payload []byte
}

func NewDataLogEvent(b []byte) DataLogEvent {
	return DataLogEvent{b}
}
