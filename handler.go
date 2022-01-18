package handler

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc"
)

type Handler interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	PutNx(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.TxnResponse, error)
	Add(ctx context.Context, prefix, key, addr string, opts ...clientv3.OpOption) error
	Dial(ctx context.Context, servicePrefix string, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	KeepAlive(ctx context.Context, leaseID clientv3.LeaseID, key string) error
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
}

type Holder struct {
	Client *clientv3.Client
	Logger zerolog.Logger
}

type Option struct {
	Logger zerolog.Logger
	*clientv3.Config
}

func New(ctx context.Context, opt *Option) (Handler, error) {
	c, err := clientv3.New(*opt.Config)
	if err != nil {
		return nil, err
	}

	return &Holder{
		Client: c,
		Logger: opt.Logger,
	}, nil
}

func (h *Holder) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return h.Client.Get(ctx, key, opts...)
}

func (h *Holder) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return h.Client.Grant(ctx, ttl)
}

func (h *Holder) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return h.Client.Put(ctx, key, val, opts...)
}

func (h *Holder) PutNx(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.TxnResponse, error) {
	return h.Client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, val, opts...)).
		Commit()
}

func (h *Holder) Add(ctx context.Context, prefix, key, addr string, opts ...clientv3.OpOption) error {
	em, err := endpoints.NewManager(h.Client, prefix)
	if err != nil {
		return err
	}

	return em.AddEndpoint(
		ctx,
		key,
		endpoints.Endpoint{Addr: addr},
		opts...,
	)
}

func (h *Holder) Dial(ctx context.Context, servicePrefix string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	etcdResolver, err := NewBuilder(ctx, h.Client)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.WithResolvers(etcdResolver), grpc.WithBlock(), grpc.WithDisableRetry(), grpc.WithInsecure())

	return grpc.DialContext(ctx, "etcd:///"+servicePrefix, opts...)
}

func (h *Holder) KeepAlive(ctx context.Context, leaseID clientv3.LeaseID, key string) error {
	ch, err := h.Client.KeepAlive(ctx, leaseID)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-ch:
			if !ok {
				return fmt.Errorf("attempts to renew the lease have failed")
			}

			h.Logger.Debug().Msgf("lease success[key=%s]", key)
		}
	}
}

func (h *Holder) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return h.Client.Delete(ctx, key, opts...)
}
