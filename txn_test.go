package crkv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxn(t *testing.T) {
	kv := NewKVStore("postgresql://root@localhost:26257?sslmode=disable", SetIsolationLevel("SNAPSHOT"))
	defer kv.Close()
	assert.Nil(t, kv.CreateDatabase("test_txn"))
	assert.Nil(t, kv.Put(context.TODO(), "foo", "bar"))

	err := kv.Txn(context.TODO(), func(ctx context.Context, txn Txn) error {
		err := txn.Put(ctx, "foo", "bar1")
		if err != nil {
			return err
		}
		b, err := txn.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar1", string(b.RawBytes))
		b, err = kv.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(b.RawBytes))
		return nil
	})
	assert.Nil(t, err)

	err = kv.Txn(context.TODO(), func(ctx context.Context, txn Txn) error {
		err := txn.Put(ctx, "foo", "bar2")
		if err != nil {
			return err
		}
		b, err := txn.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar2", string(b.RawBytes))
		b, err = kv.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar1", string(b.RawBytes))
		return nil
	})
	assert.Nil(t, err)
	err = kv.Txn(context.TODO(), func(ctx context.Context, txn Txn) error {
		err := txn.Del(ctx, "foo")
		if err != nil {
			return err
		}
		return nil
	})
	assert.Nil(t, err)
}

func TestInmemTxn(t *testing.T) {
	kv := NewInmemKVStore()
	assert.Nil(t, kv.Put(context.TODO(), "foo", "bar"))
	err := kv.Txn(context.TODO(), func(ctx context.Context, txn Txn) error {
		err := txn.Put(ctx, "foo", "bar1")
		if err != nil {
			return err
		}
		b, err := txn.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar1", string(b.RawBytes))
		b, err = kv.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(b.RawBytes))
		return nil
	})
	assert.Nil(t, err)

	err = kv.Txn(context.TODO(), func(ctx context.Context, txn Txn) error {
		err := txn.Put(ctx, "foo", "bar2")
		if err != nil {
			return err
		}
		b, err := txn.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar2", string(b.RawBytes))
		b, err = kv.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar1", string(b.RawBytes))
		return nil
	})
	assert.Nil(t, err)
	err = kv.Txn(context.TODO(), func(ctx context.Context, txn Txn) error {
		err := txn.Del(ctx, "foo")
		if err != nil {
			return err
		}
		return nil
	})
	assert.Nil(t, err)
}
