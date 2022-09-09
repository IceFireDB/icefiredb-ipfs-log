package main

import (
	"context"
	"errors"
	"fmt"
	iflog "github.com/IceFireDB/icefiredb-ipfs-log"
	"github.com/IceFireDB/icefiredb-ipfs-log/stores/levelkv"
	"github.com/abiosoft/ishell/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

func main() {
	ctx := context.TODO()
	rootPath := "./levelkv"
	node, api, err := iflog.CreateNode(ctx, rootPath)
	if err != nil {
		panic(err)
	}
	// Build host multiaddress
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", node.PeerHost.ID().Pretty()))
	for _, a := range node.PeerHost.Addrs() {
		fmt.Println(a.Encapsulate(hostAddr).String())
	}

	log := zap.NewNop()

	ev, err := iflog.NewIpfsLog(ctx, api, "/ipfs/iflog-event/levelkv", &iflog.EventOptions{
		Directory: rootPath,
		Logger:    log,
	})

	if err != nil {
		panic(err)
	}

	if err := ev.AnnounceConnect(ctx, node); err != nil {
		panic(err)
	}

	kvdb, err := levelkv.NewLevelKVDB(ctx, ev, log)
	if err != nil {
		panic(err)
	}
	if err := ev.LoadDisk(ctx); err != nil {
		panic(err)
	}

	shell := ishell.New()

	shell.AddCmd(&ishell.Cmd{
		Name: "get",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Err(errors.New("参数错误"))
				return
			}
			val, err := kvdb.Get([]byte(c.Args[0]))
			if err != nil {
				c.Err(err)
				return
			}
			if len(val) == 0 {
				c.Println("Nil")
				return
			}
			c.Println(string(val))
		},
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "set",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 2 {
				c.Err(errors.New("参数错误"))
				return
			}
			err := kvdb.Put(ctx, []byte(c.Args[0]), []byte(c.Args[1]))
			if err != nil {
				c.Err(err)
				return
			}
			c.Println("OK")
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "delete",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Err(errors.New("参数错误"))
				return
			}
			err := kvdb.Delete(ctx, []byte(c.Args[0]))
			if err != nil {
				c.Err(err)
				return
			}
			c.Println("OK")
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "list",
		Func: func(c *ishell.Context) {
			list := kvdb.List()
			for k, v := range list {
				c.Printf("%s:%s\n", k, v)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "connect",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Err(fmt.Errorf("参数错误"))
				return
			}
			bstr, err := ma.NewMultiaddr(c.Args[0])
			if err != nil {
				c.Err(err)
				return
			}
			inf, err := peer.AddrInfoFromP2pAddr(bstr)
			if err != nil {
				c.Err(err)
				return
			}
			if err := node.PeerHost.Connect(context.TODO(), *inf); err != nil {
				c.Err(err)
				return
			}
			node.PeerHost.ConnManager().TagPeer(inf.ID, "keep", 100)
			c.Println("连接成功！")
		},
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "peers",
		Func: func(c *ishell.Context) {
			slice := node.PeerHost.Peerstore().Peers()
			for _, v := range slice {
				c.Println(v.String())
			}
		},
	})

	shell.Run()
}
