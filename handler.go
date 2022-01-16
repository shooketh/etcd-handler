package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc"
)

type Handler interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	PutNx(ctx context.Context, key, value string, ttl int64) (*clientv3.TxnResponse, clientv3.LeaseID, error)
	Add(ctx context.Context, prefix, key string, ttl int64) (clientv3.LeaseID, error)
	Dial(ctx context.Context, servicePrefix string, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	KeepAlive(ctx context.Context, leaseID clientv3.LeaseID, key string) error
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error
}

type Holder struct {
	Addr   string
	Client *clientv3.Client
	Logger zerolog.Logger
	*EtcdOption
}

type Option struct {
	ServerIP   string
	ServerPort string
	Logger     zerolog.Logger
	*EtcdOption
}

type EtcdOption struct {
	Endpoints []string
	Timeout   time.Duration
	Username  string
	Password  string
}

func New(opt *Option) (Handler, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   opt.Endpoints,
		DialTimeout: opt.Timeout,
	})
	if err != nil {
		return nil, err
	}

	return &Holder{
		Addr:       opt.ServerIP + ":" + opt.ServerPort,
		Client:     c,
		Logger:     opt.Logger,
		EtcdOption: opt.EtcdOption,
	}, nil
}

func (h *Holder) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return h.Client.Get(ctx, key, opts...)
}

func (h *Holder) PutNx(ctx context.Context, key, value string, ttl int64) (*clientv3.TxnResponse, clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	leaseResp, err := h.Client.Grant(ctx, ttl)
	if err != nil {
		return nil, 0, err
	}

	res, err := h.Client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(leaseResp.ID))).
		Commit()
	if err != nil {
		return nil, 0, err
	}

	return res, leaseResp.ID, nil
}

func (h *Holder) Add(ctx context.Context, prefix, key string, ttl int64) (leaseID clientv3.LeaseID, err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	em, err := endpoints.NewManager(h.Client, prefix)
	if err != nil {
		return
	}

	leaseResp, err := h.Client.Grant(ctx, ttl)
	if err != nil {
		return
	}

	err = em.AddEndpoint(
		ctx,
		key,
		endpoints.Endpoint{Addr: h.Addr},
		clientv3.WithLease(leaseResp.ID),
	)
	if err != nil {
		return
	}

	return leaseResp.ID, nil
}

func (h *Holder) Dial(ctx context.Context, servicePrefix string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	etcdResolver, err := NewBuilder(ctx, h.Client)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.WithResolvers(etcdResolver), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithDisableRetry())

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

func (h *Holder) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error {
	_, err := h.Client.Delete(ctx, key, opts...)
	if err != nil {
		return err
	}

	return nil
}
