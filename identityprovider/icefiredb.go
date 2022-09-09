package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import (
	idp "berty.tech/go-ipfs-log/identityprovider"
	"context"
	"encoding/hex"

	"github.com/libp2p/go-libp2p-core/crypto"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/keystore"
)

func init() {
	_ = idp.AddIdentityProvider(NewIceFireDBIdentityProvider)
}

type IceFireDBIdentityProvider struct {
	keystore keystore.Interface
}

// VerifyIdentity checks an OrbitDB identity.
func (p *IceFireDBIdentityProvider) VerifyIdentity(identity *idp.Identity) error {
	return nil
}

// NewOrbitDBIdentityProvider creates a new identity for use with OrbitDB.
func NewIceFireDBIdentityProvider(options *idp.CreateIdentityOptions) idp.Interface {
	provider := &IceFireDBIdentityProvider{}
	if options != nil {
		provider.keystore = options.Keystore
	}
	return provider
}

// GetID returns the identity's ID.
func (p *IceFireDBIdentityProvider) GetID(ctx context.Context, options *idp.CreateIdentityOptions) (string, error) {
	private, err := p.keystore.GetKey(ctx, options.ID)
	if err != nil || private == nil {
		private, err = p.keystore.CreateKey(ctx, options.ID)
		if err != nil {
			return "", errmsg.ErrKeyStoreCreateEntry.Wrap(err)
		}
	}

	pubBytes, err := private.GetPublic().Raw()
	if err != nil {
		return "", errmsg.ErrPubKeySerialization.Wrap(err)
	}

	return hex.EncodeToString(pubBytes), nil
}

// SignIdentity signs an OrbitDB identity.
func (p *IceFireDBIdentityProvider) SignIdentity(ctx context.Context, data []byte, id string) ([]byte, error) {
	key, err := p.keystore.GetKey(ctx, id)
	if err != nil {
		return nil, errmsg.ErrKeyNotInKeystore
	}

	//data, _ = hex.DecodeString(hex.EncodeToString(data))

	// FIXME? Data is a unicode encoded hex as a byte (source lib uses Buffer.from(hexStr) instead of Buffer.from(hexStr, "hex"))
	data = []byte(hex.EncodeToString(data))

	signature, err := key.Sign(data)
	if err != nil {
		return nil, errmsg.ErrSigSign.Wrap(err)
	}

	return signature, nil
}

// Sign signs a value using the current.
func (p *IceFireDBIdentityProvider) Sign(ctx context.Context, identity *idp.Identity, data []byte) ([]byte, error) {
	key, err := p.keystore.GetKey(ctx, identity.ID)
	if err != nil {
		return nil, errmsg.ErrKeyNotInKeystore.Wrap(err)
	}

	sig, err := key.Sign(data)
	if err != nil {
		return nil, errmsg.ErrSigSign.Wrap(err)
	}

	return sig, nil
}

func (p *IceFireDBIdentityProvider) UnmarshalPublicKey(data []byte) (crypto.PubKey, error) {
	pubKey, err := crypto.UnmarshalSecp256k1PublicKey(data)
	if err != nil {
		return nil, errmsg.ErrInvalidPubKeyFormat
	}

	return pubKey, nil
}

// GetType returns the current identity type.
func (*IceFireDBIdentityProvider) GetType() string {
	return "icefiredb"
}

var _ idp.Interface = &IceFireDBIdentityProvider{}
