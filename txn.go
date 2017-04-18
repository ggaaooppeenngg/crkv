package crkv

import (
	"context"
	"database/sql"
)

// Txn is an in-progress distributed database transaction.
type Txn struct {
	database string
	tx       *sql.Tx
}

// Del deletes key.
func (txn *Txn) Del(ctx, key interface{}) error {
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	_, err = txn.tx.Exec("DELETE FROM "+txn.database+".kvt WHERE key = $1", kb)
	if err != nil {
		return err
	}
	return nil
}

// Get retrives a key.
func (txn *Txn) Get(ctx context.Context, key interface{}) (Value, error) {
	v := []byte{}
	err := txn.tx.QueryRowContext(ctx, "SELECT value FROM "+txn.database+".kvt WHERE key = $1", key).Scan(&v)
	if err != nil {
		return Value{}, err
	}
	return Value{
		RawBytes: v,
	}, nil
}

// Put sets a key.
func (txn *Txn) Put(ctx context.Context, key interface{}, value interface{}) error {
	kb, vb := []byte{}, []byte{}
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	vb, err = i2Bytes(value)
	if err != nil {
		return err
	}
	_, err = txn.tx.ExecContext(ctx, "UPSERT INTO "+txn.database+".kvt VALUES ($1, $2)", kb, vb)
	if err != nil {
		return err
	}
	return nil

}
