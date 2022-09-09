package kv

import (
	"context"
	"encoding/json"
	iflog "github.com/IceFireDB/icefiredb-ipfs-log"
	cache "github.com/patrickmn/go-cache"
	"go.uber.org/zap"
)

const UnDefStr = ""

type KeyValueDB struct {
	ev  *iflog.IpfsLog
	db  *cache.Cache
	log *zap.Logger
}

func NewKeyValueDB(ctx context.Context, ev *iflog.IpfsLog, log *zap.Logger) (*KeyValueDB, error) {
	db := cache.New(0, 0)
	kv := KeyValueDB{
		ev:  ev,
		db:  db,
		log: log,
	}
	if err := kv.syncData(ctx); err != nil {
		return nil, err
	}
	return &kv, nil
}

func (kv *KeyValueDB) syncData(ctx context.Context) error {
	ch, err := kv.ev.WatchWriteEvent(ctx)
	if err != nil {
		return err
	}
	go func() {
		var data []byte
		var opr operation
		for {
			select {
			case <-ctx.Done():
				return
			case data = <-ch:
			}
			err = json.Unmarshal(data, &opr)
			if err != nil {
				kv.log.Error("json Unmarshal sync data err:", zap.Error(err))
				continue
			}
			switch opr.Op {
			case SetOperateType:
				kv.db.Set(opr.Key, string(opr.Value), -1)
			case DeleteOperateType:
				kv.db.Delete(opr.Key)
			}
		}
	}()
	return nil
}

func (kv *KeyValueDB) Get(key string) string {
	val, ok := kv.db.Get(key)
	if !ok {
		return UnDefStr
	}
	return val.(string)
}

func (kv *KeyValueDB) Put(ctx context.Context, key, value string) error {
	op := newOperation(key, []byte(value), SetOperateType)
	_, err := kv.ev.Append(ctx, op.marshal())
	if err != nil {
		return err
	}
	kv.db.Set(key, value, -1)
	return nil
}

func (kv *KeyValueDB) Delete(ctx context.Context, key string) error {
	op := newOperation(key, nil, DeleteOperateType)
	_, err := kv.ev.Append(ctx, op.marshal())
	if err != nil {
		return err
	}
	kv.db.Delete(key)
	return nil
}

func (kv *KeyValueDB) List() map[string]string {
	list := make(map[string]string)

	for k, v := range kv.db.Items() {
		list[k] = v.Object.(string)
	}
	return list
}

func (kv *KeyValueDB) Flush(ctx context.Context) error {
	for k := range kv.db.Items() {
		if err := kv.Delete(ctx, k); err != nil {
			return err
		}
		kv.db.Delete(k)
	}
	return nil
}
