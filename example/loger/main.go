package main

import (
	"context"
	"fmt"
	"log"
	"os"

	iflog "github.com/IceFireDB/icefiredb-ipfs-log"
	ishell "github.com/abiosoft/ishell/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/kubo/core"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

var logger *log.Logger

func init() {
	f, err := os.OpenFile("./debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	logger = log.New(f, "", log.Lshortfile|log.Ldate)
}

func main() {
	ctx := context.TODO()
	rootPath := "./data"
	node, api, err := iflog.CreateNode(ctx, rootPath)
	if err != nil {
		panic(err)
	}
	PrintHostAddress(node.PeerHost)
	ev, err := iflog.NewIpfsLog(ctx, api, "iflog-event", &iflog.EventOptions{
		Directory: rootPath,
	})
	if err != nil {
		panic(err)
	}

	if err := ev.AnnounceConnect(ctx, node); err != nil {
		panic(err)
	}

	go func() {
		ch, err := ev.WatchWriteEvent(ctx)
		if err != nil {
			panic(err)
		}
		for e := range ch {
			logger.Println(string(e))
		}
	}()

	if err := ev.LoadDisk(ctx); err != nil {
		panic(err)
	}

	shell := ishell.New()
	addCmd(ctx, shell, node, ev)
	shell.Run()
}

func addCmd(ctx context.Context, shell *ishell.Shell, node *core.IpfsNode, ev *iflog.IpfsLog) {
	shell.AddCmd(&ishell.Cmd{
		Name: "append",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Err(fmt.Errorf("Parameter error"))
				return
			}
			field := c.Args[0]
			id, err := ev.Append(ctx, []byte(field))
			if err != nil {
				c.Err(err)
				return
			}

			c.Println(id.String())
		},
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "get",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Err(fmt.Errorf("Parameter error"))
				return
			}
			hash := c.Args[0]
			id, err := cid.Decode(hash)
			if err != nil {
				c.Err(err)
				return
			}
			data := ev.Get(id)
			c.Println(data)
		},
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "list",
		Func: func(c *ishell.Context) {
			data := ev.List()
			for _, v := range data {
				c.Println(string(v))
			}
		},
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "connect",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Err(fmt.Errorf("Parameter error"))
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
			c.Println("connection succeeded!")
		},
	})
}

func PrintHostAddress(ha host.Host) {
	// Build host multiaddress
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", ha.ID().Pretty()))

	// Now we can build a full multiaddress to reach this host
	// by encapsulating both addresses:
	for _, a := range ha.Addrs() {
		fmt.Println(a.Encapsulate(hostAddr).String())
	}
}
