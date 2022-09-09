package levelkv

import (
	"context"
	"encoding/json"
	iflog "github.com/IceFireDB/icefiredb-ipfs-log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
)

type LevelKV struct {
	ev  *iflog.IpfsLog
	db  *leveldb.DB
	log *zap.Logger
}

func NewLevelKVDB(ctx context.Context, ev *iflog.IpfsLog, log *zap.Logger) (*LevelKV, error) {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return nil, err
	}
	kv := LevelKV{
		ev:  ev,
		db:  db,
		log: log,
	}
	if err := kv.syncData(ctx); err != nil {
		return nil, err
	}
	return &kv, nil
}

func (kv *LevelKV) syncData(ctx context.Context) error {
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
				_ = kv.db.Put(opr.Key, opr.Value, nil)
			case DeleteOperateType:
				_ = kv.db.Delete(opr.Key, nil)
			}
		}
	}()
	return nil
}

func (kv *LevelKV) Get(key []byte) ([]byte, error) {
	val, err := kv.db.Get(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		return nil, nil
	}
	return val, err
}

func (kv *LevelKV) Put(ctx context.Context, key, value []byte) error {
	op := newOperation(key, value, SetOperateType)
	_, err := kv.ev.Append(ctx, op.marshal())
	if err != nil {
		return err
	}
	return kv.db.Put(key, value, nil)
}

func (kv *LevelKV) Delete(ctx context.Context, key []byte) error {
	op := newOperation(key, nil, DeleteOperateType)
	_, err := kv.ev.Append(ctx, op.marshal())
	if err != nil {
		return err
	}
	return kv.db.Delete(key, nil)
}

func (kv *LevelKV) Has(key []byte) bool {
	ok, _ := kv.db.Has(key, nil)
	return ok
}

func (kv *LevelKV) List() map[string]string {
	ite := kv.db.NewIterator(nil, &opt.ReadOptions{})
	list := make(map[string]string)
	for {
		if !ite.Next() {
			break
		}
		list[string(ite.Key())] = string(ite.Value())
	}
	return list
}

func (kv *LevelKV) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	return kv.db.NewIterator(slice, ro)
}

func (kv *LevelKV) GetSnapshot() (*leveldb.Snapshot, error) {
	return kv.db.GetSnapshot()
}
