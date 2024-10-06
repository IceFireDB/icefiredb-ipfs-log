package icefiredb_ipfs_log

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ifacelog "berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/keystore"
	"github.com/IceFireDB/icefiredb-ipfs-log/cache"
	"github.com/IceFireDB/icefiredb-ipfs-log/cache/cacheleveldown"
	_ "github.com/IceFireDB/icefiredb-ipfs-log/identityprovider"
	"github.com/IceFireDB/icefiredb-ipfs-log/pubsub"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipfscore "github.com/ipfs/kubo/core"
	iface "github.com/ipfs/kubo/core/coreiface"
	"github.com/libp2p/go-libp2p/core"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	defaultCacheKey = datastore.NewKey("_latestHeads")
)

type IpfsLog struct {
	peerID        core.PeerID
	ipfs          iface.CoreAPI
	pubsub        pubsub.PubSubInterface
	eventBus      event.Bus
	log           *zap.Logger
	keystore      keystore.Interface
	closeKeystore func() error
	ipfslog       ipfslog.Log
	joinLock      sync.Mutex
	identity      *idp.Identity
	dbname        string
	io            ipfslog.IO
	cache         datastore.Datastore
	emitters      struct {
		localWrite  event.Emitter // Local write Data
		remoteWrite event.Emitter
		dataLog     event.Emitter // Write data events to notify upper levels
	}
}

type EventOptions struct {
	Directory     string
	Keystore      keystore.Interface
	CloseKeystore func() error
	Logger        *zap.Logger
	Identity      *idp.Identity
	IpfsLog       ipfslog.Log
	Pubsub        pubsub.PubSubInterface
	IO            ipfslog.IO
	Cache         datastore.Datastore
}

func NewIpfsLog(ctx context.Context, ipfs iface.CoreAPI, dbname string, options *EventOptions) (*IpfsLog, error) {
	if ipfs == nil {
		return nil, errors.New("ipfs is a required argument")
	}
	if len(dbname) == 0 {
		return nil, errors.New("db name is a required argument")
	}

	k, err := ipfs.Key().Self(ctx)
	if err != nil {
		return nil, err
	}
	peerID := k.ID()

	if options == nil {
		options = &EventOptions{}
	}
	if options.Logger == nil {
		options.Logger = zap.NewNop()
	}

	if options.Keystore == nil {
		var err error
		var ds *leveldb.Datastore
		// create new datastore
		if options.Directory == cacheleveldown.InMemoryDirectory {
			ds, err = leveldb.NewDatastore("", nil)
		} else {
			ds, err = leveldb.NewDatastore(path.Join(options.Directory, peerID.String(), "/keystore"), nil)
		}

		if err != nil {
			return nil, errors.Wrap(err, "unable to create data store used by keystore")
		}

		ks, err := keystore.NewKeystore(ds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create keystore")
		}

		options.Keystore = ks
		options.CloseKeystore = ds.Close
	}
	if options.Identity == nil {
		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: options.Keystore,
			Type:     "icefiredb",
			ID:       peerID.String(),
		})

		if err != nil {
			return nil, err
		}
		options.Identity = identity
	}
	if options.IO == nil {
		options.IO = io.CBOR()
	}

	if options.IpfsLog == nil {
		ilog, err := ipfslog.NewLog(ipfs, options.Identity, &ipfslog.LogOptions{
			ID: dbname,
			IO: options.IO,
		})
		if err != nil {
			return nil, err
		}

		options.IpfsLog = ilog
	}
	if options.Pubsub == nil {
		options.Pubsub = pubsub.NewPubSub(ipfs, peerID, time.Second*2, options.Logger)
	}
	if options.IO == nil {
		options.IO = io.CBOR()
	}
	// Disk caches key indexes, which are recovered from IPFS upon restart
	if options.Cache == nil {
		c := cacheleveldown.New(&cache.Options{Logger: options.Logger})
		options.Cache, err = c.Load(options.Directory, dbname)
		if err != nil {
			return nil, err
		}
	}
	ev := IpfsLog{
		peerID:        peerID,
		ipfs:          ipfs,
		pubsub:        options.Pubsub,
		log:           options.Logger,
		keystore:      options.Keystore,
		closeKeystore: options.CloseKeystore,
		identity:      options.Identity,
		ipfslog:       options.IpfsLog,
		io:            options.IO,
		dbname:        dbname,
		cache:         options.Cache,
	}
	topic, err := ev.pubsub.TopicSubscribe(ctx, ev.dbname)
	if err != nil {
		return nil, err
	}
	ev.eventBus = eventbus.NewBus()

	if err := ev.generateEmitter(); err != nil {
		return nil, err
	}
	if err := ev.peerListener(ctx, topic); err != nil {
		return nil, err
	}
	if err := ev.storeListener(ctx, topic); err != nil {
		return nil, err
	}

	return &ev, nil
}

func (ev *IpfsLog) AnnounceConnect(ctx context.Context, node *ipfscore.IpfsNode) error {
	// Announce that this host can provide the service CID
	hash, err := generateCID(ev.dbname)
	if err != nil {
		return err
	}

	err = node.DHT.Provide(ctx, hash, true)
	if err != nil {
		return errors.Wrap(err, "Failed to Announce Service CID!")
	}
	// Find the other providers for the service CID
	peerchan := node.DHT.FindProvidersAsync(ctx, hash, 0)

	go func() {
		// Iterate over the peer channel
		for pe := range peerchan {
			// Ignore if the discovered peer is the host itself
			if pe.ID == ev.peerID {
				continue
			}
			if len(pe.Addrs) == 0 {
				continue
			}
			go func(p peer.AddrInfo) {
				// Connect to the peer
				err := ev.ipfs.Swarm().Connect(context.TODO(), p)
				if err != nil {
					ev.log.Error(fmt.Sprintf("p2p peer %s connection failed: %v", p.String()), zap.Error(err))
				} else {
					ev.log.Debug(fmt.Sprintf("p2p peer connection success: ", p.String()))
				}
			}(pe)
		}
	}()
	return nil
}

// Load disk cache data
func (ev *IpfsLog) LoadDisk(ctx context.Context) error {
	val, err := ev.cache.Get(ctx, defaultCacheKey)
	if err != nil && err != datastore.ErrNotFound {
		return err
	}
	err = nil

	if len(val) == 0 {
		return nil
	}

	var heads []*entry.Entry
	err = json.Unmarshal(val, &heads)
	if err != nil {
		return errors.Wrap(err, "json unmarshal heads error")
	}
	hs := make([]ifacelog.IPFSLogEntry, len(heads))
	for k, v := range heads {
		hs[k] = v
	}

	l, err := ipfslog.NewFromEntry(ctx, ev.ipfs, ev.identity, hs, &ifacelog.LogOptions{
		ID: ev.ipfslog.GetID(),
		IO: ev.io,
	}, &ifacelog.FetchOptions{
		ShouldExclude: func(hash cid.Cid) bool {
			_, ok := ev.ipfslog.Get(hash)
			return ok
		},
	})

	if err != nil {
		return err
	}

	if _, inErr := ev.ipfslog.Join(l, -1); inErr != nil {
		ev.log.Error("join error:", zap.Error(inErr))
		return err
	}

	for _, v := range l.GetEntries().Slice() {
		if err := ev.emitters.dataLog.Emit(NewDataLogEvent(v.GetPayload())); err != nil {
			return err
		}
	}
	return nil
}

func (ev *IpfsLog) generateEmitter() error {
	var err error
	if ev.emitters.localWrite, err = ev.eventBus.Emitter(new(LocalWrite)); err != nil {
		return errors.Wrap(err, "unable to create EventWrite emitter")
	}
	if ev.emitters.remoteWrite, err = ev.eventBus.Emitter(new(RemoteWrite)); err != nil {
		return errors.Wrap(err, "unable to create EventWrite emitter")
	}
	if ev.emitters.dataLog, err = ev.eventBus.Emitter(new(DataLogEvent)); err != nil {
		return errors.Wrap(err, "unable to create EventWrite emitter")
	}
	return err
}

func (ev *IpfsLog) Close() error {
	ev.cache.Close()
	return ev.closeKeystore()
}

// Process local write commands and broadcast them to peers
func (ev *IpfsLog) storeListener(ctx context.Context, topic pubsub.PubSubTopic) error {
	sub, err := ev.eventBus.Subscribe(new(LocalWrite))
	if err != nil {
		return err
	}
	go func() {
		for {
			var e interface{}
			select {
			case <-ctx.Done():
				return
			case e = <-sub.Out():
			}

			evt := e.(LocalWrite)
			go func() {
				tctx, cancel := context.WithTimeout(ctx, time.Second*10)
				defer cancel()

				if err := ev.handleEventWrite(tctx, &evt, topic); err != nil {
					ev.log.Warn("unable to handle EventWrite", zap.Error(err))
				}
			}()
		}
	}()
	return nil
}

func (ev *IpfsLog) handleEventWrite(ctx context.Context, e *LocalWrite, topic pubsub.PubSubTopic) error {
	if len(e.Heads) == 0 {
		return fmt.Errorf("'heads' are not defined")
	}

	if topic != nil {
		peer, err := topic.Peers(ctx)
		if err != nil {
			return fmt.Errorf("unable to get topic peers: %w", err)
		}
		if len(peer) > 0 {
			err = topic.Publish(ctx, e.Marshal())
			if err != nil {
				return fmt.Errorf("unable to publish message on pubsub %w", err)
			}
		}
	}
	return nil
}

// Listen for nodes to join
// Listens for peer messages
func (ev *IpfsLog) peerListener(ctx context.Context, topic pubsub.PubSubTopic) error {
	chPeers, err := topic.WatchPeers(ctx)
	if err != nil {
		return err
	}

	chMessages, err := topic.WatchMessages(ctx)
	if err != nil {
		return err
	}

	go func() {
		for e := range chPeers {
			switch evt := e.(type) {
			case *pubsub.EventPubSubJoin:
				go ev.handleNewPeerJoined(ctx, evt.Peer, topic)
			case *pubsub.EventPubSubLeave:
			default:
			}
		}
	}()

	go func() {
		for e := range chMessages {
			var msg RemoteWrite
			if err := json.Unmarshal(e.Content, &msg); err != nil {
				ev.log.Error("unable to unmarshal head entries", zap.Error(err))
				continue
			}
			if len(msg.Heads) == 0 {
				continue
			}
			if err := ev.sync(ctx, &msg, -1); err != nil {
				ev.log.Error(fmt.Sprintf("Error while syncing heads for %s:", e.From.String()))
				continue
			}
		}
	}()
	return nil
}

// A new node joins and pushes its own data
func (ev *IpfsLog) handleNewPeerJoined(ctx context.Context, peer core.PeerID, topic pubsub.PubSubTopic) {
	head := ev.ipfslog.Heads().Slice()
	hs := make([]*entry.Entry, len(head))
	for k, v := range head {
		hs[k] = v.(*entry.Entry)
	}
	msg := NewRemoteWrite(ev.peerID, hs)
	if err := topic.Publish(ctx, msg.Marshal()); err != nil {
		ev.log.Error("new peer publish entry err:", zap.Error(err))
	}
}

// Peer message merging
func (ev *IpfsLog) sync(ctx context.Context, msg *RemoteWrite, amount int) error {
	ev.joinLock.Lock()
	defer ev.joinLock.Unlock()

	logList := []ipfslog.Log{}
	logLock := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(msg.Heads))
	for _, h := range msg.Heads {
		if h == nil {
			continue
		}

		if h.GetNext() == nil {
			h.SetNext([]cid.Cid{})
		}
		if h.GetRefs() == nil {
			h.SetRefs([]cid.Cid{})
		}

		identityProvider := ev.identity.Provider
		if identityProvider == nil {
			return errors.New("identity-provider is required, cannot verify entry")
		}

		hash, err := ev.io.Write(ctx, ev.ipfs, h, nil)
		if err != nil {
			return errors.Wrap(err, "unable to write entry on dag")
		}

		if hash.String() != h.GetHash().String() {
			return errors.New("WARNING! Head hash didn't match the contents")
		}
		go func(ety *entry.Entry) {
			defer wg.Done()

			l, err := ipfslog.NewFromEntryHash(ctx, ev.ipfs, ev.identity, ety.Hash, &ifacelog.LogOptions{
				ID: ev.ipfslog.GetID(),
				IO: ev.io,
			}, &ipfslog.FetchOptions{
				ShouldExclude: func(hash cid.Cid) bool {
					_, ok := ev.ipfslog.Get(hash)
					return ok
				},
				// ProgressChan: cprogress,
			})
			if err != nil {
				ev.log.Error("data log emit err:", zap.Error(err))
				return
			}
			logLock.Lock()
			logList = append(logList, l)
			logLock.Unlock()

		}(h)

	}
	wg.Wait()
	if len(logList) > 0 {
		otherLog, err := ev.mergeLogs(logList)
		if err != nil {
			return err
		}
		_, err = ev.ipfslog.Join(otherLog, -1)
		if err != nil {
			return err
		}
		for _, v := range otherLog.Values().Slice() {
			if err := ev.emitters.dataLog.Emit(NewDataLogEvent(v.GetPayload())); err != nil {
				ev.log.Error("data log emit err:", zap.Error(err))
			}
		}
	}
	return ev.reloadLastHeadCache(ctx)
}

func (ev *IpfsLog) mergeLogs(logs []ipfslog.Log) (ipfslog.Log, error) {
	if len(logs) == 0 {
		return nil, nil
	}
	if len(logs) == 1 {
		return logs[0], nil
	}
	logA := logs[0]
	for _, v := range logs[1:] {
		_, err := logA.Join(v, -1)
		if err != nil {
			return nil, err
		}
	}
	return logA, nil
}

func (ev *IpfsLog) Append(ctx context.Context, payload []byte) (cid.Cid, error) {
	entryLog, err := ev.ipfslog.Append(ctx, payload, &ipfslog.AppendOptions{
		//Pin: true,
		PointerCount: 64,
	})
	if err != nil {
		return cid.Undef, err
	}

	heads := ev.ipfslog.Heads().Slice()

	hs := make([]*entry.Entry, len(heads))
	for k, v := range heads {
		hs[k] = v.(*entry.Entry)
	}

	if err = ev.emitters.localWrite.Emit(NewLocalWrite(hs)); err != nil {
		ev.log.Error("emit evtwrite msg error:", zap.Error(err))
		return cid.Undef, err
	}

	if err = ev.reloadLastHeadCache(ctx); err != nil {
		return cid.Undef, err
	}
	return entryLog.GetHash(), nil
}

func (ev *IpfsLog) reloadLastHeadCache(ctx context.Context) error {
	slice := ev.ipfslog.Heads().Slice()
	headEntry, err := json.Marshal(slice)
	if err != nil {
		return err
	}
	return ev.cache.Put(ctx, defaultCacheKey, headEntry)
}

func (ev *IpfsLog) Get(cid cid.Cid) []byte {
	entryLog, ok := ev.ipfslog.Get(cid)
	if !ok {
		return nil
	}
	return entryLog.GetPayload()
}

func (ev *IpfsLog) List() [][]byte {
	entryLog := ev.ipfslog.Values()
	data := make([][]byte, entryLog.Len())
	for k, v := range entryLog.Slice() {
		data[k] = v.GetPayload()
	}
	return data
}

func (ev *IpfsLog) WatchWriteEvent(ctx context.Context) (<-chan []byte, error) {
	ch := make(chan []byte)
	sub, err := ev.eventBus.Subscribe(new(DataLogEvent))
	if err != nil {
		return nil, err
	}
	go func() {
		defer sub.Close()
		for {
			var e interface{}
			select {
			case <-ctx.Done():
				return
			case e = <-sub.Out():
			}
			evt := e.(DataLogEvent)
			if len(evt.Payload) == 0 {
				continue
			}
			ch <- evt.Payload
		}
	}()
	return ch, nil
}
