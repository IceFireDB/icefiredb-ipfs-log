# icefiredb-ipfs-log

icefiredb-ipfs-log is a distributed immutable, operation-based conflict-free replication data structure that relies on ipfs to store data and merges each peer node data based on pubsub conflict-free.
You can easily implement custom data structures such as kv, event, nosql, etc. based on icefiredb-ipfs-log.

### Conflict-free log replication model
```shell
           Log A                Log B
             |                    |
     logA.append("one")   logB.append("hello")
             |                    |
             v                    v
          +-----+             +-------+
          |"one"|             |"hello"|
          +-----+             +-------+
             |                    |
     logA.append("two")   logB.append("world")
             |                    |
             v                    v
       +-----------+       +---------------+
       |"one","two"|       |"hello","world"|
       +-----------+       +---------------+
             |                    |
             |                    |
       logA.join(logB) <----------+
             |
             v
+---------------------------+
|"one","hello","two","world"|
+---------------------------+
```

### Example of building a key-value database using icefiredb-ipfs-log
- memory key-value：[memory-kv](./stores/kv/db.go)
- leveldb kv      ：[leveldb-kv](./stores/levelkv/db.go)

### Use of key-value databases
[Detailed usage example reference](./example)
```go
func main() {
    ctx := context.TODO()
	// disk cache directory
    rootPath := "./kvdb"
    node, api, err := iflog.CreateNode(ctx, rootPath)
    if err != nil {
        panic(err)
    }
	
    hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", node.PeerHost.ID().Pretty()))
    for _, a := range node.PeerHost.Addrs() {
        fmt.Println(a.Encapsulate(hostAddr).String())
    }
    
    log := zap.NewNop()
	dbname := "iflog-event-kv"
    ev, err := iflog.NewIpfsLog(ctx, api, dbname, &iflog.EventOptions{
    Directory: rootPath,
        Logger:    log,
    })
    if err != nil {
        panic(err)
    }
	
    if err := ev.AnnounceConnect(ctx, node); err != nil {
        panic(err)
    }
    kvdb, err := kv.NewKeyValueDB(ctx, ev, log)
    if err != nil {
        panic(err)
    }
    // Load old data from disk
    if err := ev.LoadDisk(ctx); err != nil {
        panic(err)
    }
	kvdb.Put(ctx, "one", "one")
	kvdb.Get("one")
	kvdb.Delete(ctx, "one")
}
```

### Some code reference sources
- [go-ipfs-log](https://github.com/berty/go-ipfs-log)

## License
icefiredb-ipfs-log is under the Apache 2.0 license. See the LICENSE directory for details.