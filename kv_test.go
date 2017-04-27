package crkv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKV(t *testing.T) {
	kv := NewKVStore("postgresql://root@localhost:26257?sslmode=disable")
	defer kv.Close()
	assert.Nil(t, kv.CreateDatabase("test"))
	assert.Nil(t, kv.Put(context.TODO(), "foo", "bar"))
	b, err := kv.Get(context.TODO(), "foo")
	assert.Nil(t, err)
	assert.Equal(t, "bar", string(b.RawBytes))
	assert.Nil(t, kv.Put(context.TODO(), "foo0", "bar0"))
	assert.Nil(t, kv.Put(context.TODO(), "foo1", "bar1"))
	assert.Nil(t, kv.Put(context.TODO(), "foo2", "bar2"))
	kvps, err := kv.Scan(context.TODO(), "foo0", "foo3", 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvps))
	err = kv.Del(context.TODO(), "foo0")
	assert.Nil(t, err)
	_, err = kv.Get(context.TODO(), "foo0")
	assert.NotNil(t, err)
}

func TestInmemKV(t *testing.T) {
	kv := NewInmemKVStore()
	assert.Nil(t, kv.Put(context.TODO(), "foo", "bar"))
	b, err := kv.Get(context.TODO(), "foo")
	assert.Nil(t, err)
	assert.Equal(t, "bar", string(b.RawBytes))
	assert.Nil(t, kv.Put(context.TODO(), "foo0", "bar0"))
	assert.Nil(t, kv.Put(context.TODO(), "foo1", "bar1"))
	assert.Nil(t, kv.Put(context.TODO(), "foo2", "bar2"))
	kvps, err := kv.Scan(context.TODO(), "foo0", "foo3", 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvps))
	err = kv.Del(context.TODO(), "foo0")
	assert.Nil(t, err)
	_, err = kv.Get(context.TODO(), "foo0")
	assert.NotNil(t, err)

}
