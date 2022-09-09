package kv

import "encoding/json"

type OperateType byte

const (
	SetOperateType OperateType = iota
	DeleteOperateType
)

type operation struct {
	Key   string      `json:"key,omitempty"`
	Value []byte      `json:"value,omitempty"`
	Op    OperateType `json:"op"`
}

func (o *operation) marshal() []byte {
	b, _ := json.Marshal(o)
	return b
}

func newOperation(key string, value []byte, op OperateType) operation {
	return operation{
		Key:   key,
		Op:    op,
		Value: value,
	}
}
