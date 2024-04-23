package icefiredb_ipfs_log

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	ifGoIpfsCore "github.com/ipfs/kubo/core/coreiface"
	"github.com/ipfs/kubo/core/coreiface/options"
	"github.com/ipfs/kubo/core/node/libp2p"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo/fsrepo"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multihash"
	"github.com/pkg/errors"
)

func CreateNode(ctx context.Context, repoPath string) (*core.IpfsNode, ifGoIpfsCore.CoreAPI, error) {
	if err := SetupPlugins(repoPath); err != nil {
		return nil, nil, err
	}
	if !fsrepo.IsInitialized(repoPath) {
		var err error
		var identity config.Identity
		identity, err = config.CreateIdentity(os.Stdout, []options.KeyGenerateOption{
			options.Key.Type(options.RSAKey),
		})

		if err != nil {
			panic(err)
		}
		conf, err := config.InitWithIdentity(identity)
		if err != nil {
			panic(err)
		}
		if err := fsrepo.Init(repoPath, conf); err != nil {
			return nil, nil, err
		}
	}

	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: libp2p.DHTClientOption, // DHTOption
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	}
	if repo, err := fsrepo.Open(repoPath); err == nil {
		nodeOptions.Repo = repo
	} else {
		return nil, nil, err
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		return nil, nil, err
	}

	coreAPI, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, nil, err
	}

	return node, coreAPI, nil
}

func SetupPlugins(path string) error {
	plugins, err := loader.NewPluginLoader(filepath.Join(path, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

// A function that generates a CID object for a given string and returns it.
// Uses SHA256 to hash the string and generate a multihash from it.
// The mulithash is then base58 encoded and then used to create the CID
func generateCID(namestring string) (cid.Cid, error) {
	// Hash the service content ID with SHA256
	hash := sha256.Sum256([]byte(namestring))
	// Append the hash with the hashing codec ID for SHA2-256 (0x12),
	// the digest size (0x20) and the hash of the service content ID
	finalhash := append([]byte{0x12, 0x20}, hash[:]...)
	// Encode the fullhash to Base58
	b58string := base58.Encode(finalhash)

	// Generate a Multihash from the base58 string
	mulhash, err := multihash.FromB58String(string(b58string))
	if err != nil {
		return cid.Undef, errors.Wrap(err, "Failed to Generate Service CID!")
	}

	// Generate a CID from the Multihash
	cidvalue := cid.NewCidV1(12, mulhash)
	// Return the CID
	return cidvalue, nil
}
