package crkv

import (
	"context"
)

type Store interface {
	Put(ctx context.Context, key interface{}, value interface{}) error
	Del(ctx context.Context, key interface{}) error
	Get(ctx context.Context, key interface{}) (Value, error)
	Scan(ctx context.Context, begin, end interface{}, max int64) ([]KeyValuePair, error)
	Txn(ctx context.Context, fn func(ctx context.Context, txn Txn) error) (err error)
}

type Txn interface {
	Del(ctx context.Context, key interface{}) error
	Get(ctx context.Context, key interface{}) (Value, error)
	Put(ctx context.Context, key interface{}, value interface{}) error
}
