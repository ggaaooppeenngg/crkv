package crkv

import (
	"context"
	"errors"

	"github.com/hashicorp/go-memdb"
)

type InmemTxn struct {
	table string
	*memdb.Txn
}

func (txn *InmemTxn) Del(ctx context.Context, key interface{}) error {
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	err = txn.Txn.Delete(txn.table, InmemKeyValuePair{
		Key: string(kb),
	})
	return err
}

func (txn *InmemTxn) Get(ctx context.Context, key interface{}) (Value, error) {
	kb, err := i2Bytes(key)
	if err != nil {
		return Value{}, err
	}
	ikvp, err := txn.Txn.First(txn.table, "id", string(kb))
	if err != nil {
		return Value{}, err
	}
	if ikvp == nil {
		return Value{}, errors.New("not exists")
	}
	return ikvp.(InmemKeyValuePair).Value, nil
}

func (txn *InmemTxn) Put(ctx context.Context, key interface{}, value interface{}) error {
	kb, vb := []byte{}, []byte{}
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	vb, err = i2Bytes(value)
	if err != nil {
		return err
	}
	err = txn.Txn.Insert(txn.table, InmemKeyValuePair{
		Key:   string(kb),
		Value: Value{RawBytes: vb},
	})
	return nil
}
