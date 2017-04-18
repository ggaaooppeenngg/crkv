// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Andrei Matei (andrei@cockroachlabs.com)
// Author: Peng Gao (peng.gao.dut@gmail.com)

package crkv

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/gogo/protobuf/proto"
	"github.com/lib/pq"
)

// Key is bytes key.
type Key struct {
	RawBytes []byte
}

// String returns string of underlying bytes.
func (k *Key) String() string {
	return string(k.RawBytes)
}

// Value is bytes value.
type Value struct {
	RawBytes []byte
}

// ValueProto unmarshals bytes to protobuf message.
func (v *Value) ValueProto(message proto.Message) error {
	err := proto.Unmarshal(v.RawBytes, message)
	return err
}

// String returns string of underlying bytes.
func (v *Value) String() string {
	return string(v.RawBytes)
}

// KeyValuePair represents a bytes key/valur pair.
type KeyValuePair struct {
	Key
	Value
}

// KVStore is a store interface to manipulate key/values.
type KVStore struct {
	globalIsolation string

	database string
	table    string
	*sql.DB
}

// NewKVStore returns a new KVStore connecting to url,
// default isolation level is serializable.
func NewKVStore(url string, options ...Option) *KVStore {
	kv := &KVStore{
		globalIsolation: "SERIALIZABLE",
		table:           "kvt",
	}
	kv.Connect(url)
	for _, opt := range options {
		opt(kv)
	}
	return kv
}

// Option is a function to set KVStore option.
type Option func(kv *KVStore)

// SetMaxConns sets connection pool size.
func SetMaxConns(n int) Option {
	return func(kv *KVStore) {
		kv.DB.SetMaxOpenConns(n)
	}
}

// SetIsolationLevel sets default isolation level.
func SetIsolationLevel(isolationLeve string) Option {
	return func(kv *KVStore) {
		kv.globalIsolation = isolationLeve
	}
}

// Connect connects to url.
func (kv *KVStore) Connect(url string) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		log.Fatal(err)
	}
	if kv.DB != nil {
		err := kv.DB.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	kv.DB = db
}

// Close closes database connections.
func (kv *KVStore) Close() error {
	return kv.DB.Close()
}

// CreateDatabase creates a new database.
func (kv *KVStore) CreateDatabase(database string) error {
	if _, err := kv.DB.Exec("CREATE DATABASE IF NOT EXISTS " + database); err != nil {
		return err
	}
	if _, err := kv.DB.Exec("SET DATABASE = " + database); err != nil {
		return err
	}
	if _, err := kv.DB.Exec("SET DEFAULT_TRANSACTION_ISOLATION TO SNAPSHOT"); err != nil {
		return err
	}
	// when database created, a default table will be created
	if _, err := kv.DB.Exec("CREATE TABLE IF NOT EXISTS kvt (key BYTES PRIMARY KEY, value BYTES)"); err != nil {
		return err
	}
	kv.database = database
	return nil
}

// Put sets a value for a key.
func (kv *KVStore) Put(ctx context.Context, key interface{}, value interface{}) error {
	kb, vb := []byte{}, []byte{}
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	vb, err = i2Bytes(value)
	if err != nil {
		return err
	}
	_, err = kv.ExecContext(ctx, "UPSERT INTO "+
		fmt.Sprintf("%s.%s", kv.database, kv.table)+
		" VALUES ($1, $2)", kb, vb)
	if err != nil {
		return err
	}
	return nil
}

// Del deletes a value for a key.
func (kv *KVStore) Del(ctx context.Context, key interface{}) error {
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	_, err = kv.ExecContext(ctx, "DELETE FROM "+
		fmt.Sprintf("%s.%s", kv.database, kv.table)+
		" WHERE key = $1", kb)
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves the value for a key.
func (kv *KVStore) Get(ctx context.Context, key interface{}) (Value, error) {
	v := []byte{}
	err := kv.QueryRowContext(ctx, "SELECT value FROM "+
		fmt.Sprintf("%s.%s", kv.database, kv.table)+
		" WHERE key = $1", key).Scan(&v)
	if err != nil {
		return Value{}, err
	}
	return Value{
		RawBytes: v,
	}, nil
}

// Scan retrieves the key/value pairs between begin
// (inclusive) and end (exclusive) in ascending order.
func (kv *KVStore) Scan(ctx context.Context, begin, end interface{}, max int64) ([]KeyValuePair, error) {
	bb, err := i2Bytes(begin)
	if err != nil {
		return nil, nil
	}
	eb, err := i2Bytes(end)
	if err != nil {
		return nil, nil
	}
	rows, err := kv.QueryContext(ctx, "SELECT key, value FROM "+
		fmt.Sprintf("%s.%s", kv.database, kv.table)+
		" WHERE key >= $1 AND key < $2 LIMIT $3", bb, eb, max)
	if err != nil {
		return nil, err
	}
	kvps := []KeyValuePair{}
	for rows.Next() {
		kvp := KeyValuePair{}
		err := rows.Scan(&kvp.Key.RawBytes, &kvp.Value.RawBytes)
		if err != nil {
			return nil, err
		}
		kvps = append(kvps, kvp)
	}
	return kvps, err
}

// Txn begins a txn running fn inside a transaction and retries it if needed.
// On non-retryable failures, the transaction is aborted and rolled
// back; on success, the transaction is committed.
// There are cases where the state of a transaction is inherently ambiguous: if
// we err on RELEASE with a communication error it's unclear if the transaction
// has been committed or not (similar to erroring on COMMIT in other databases).
// In that case, we return AmbiguousCommitError.
//
// For more information about CockroachDB's transaction model see
// https://cockroachlabs.com/docs/transactions.html.
//
// NOTE: the supplied exec closure should not have external side
// effects beyond changes to the database.
func (kv *KVStore) Txn(ctx context.Context, fn func(ctx context.Context, txn *Txn) error) (err error) {
	tx := new(sql.Tx)
	tx, err = kv.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL " + kv.globalIsolation); err != nil {
		return err
	}
	defer func() {
		if err == nil {
			// Ignore commit errors. The tx has already been committed by RELEASE.
			_ = tx.Commit()
		} else {
			// We always need to execute a Rollback() so sql.DB releases the
			// connection.
			_ = tx.Rollback()
		}
	}()

	for {
		released := false
		err = fn(ctx, &Txn{
			database: kv.database,
			tx:       tx,
		})
		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			released = true
			if _, err = tx.Exec("RELEASE SAVEPOINT cockroach_restart"); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart. We look
		// for either the standard PG errcode SerializationFailureError:40001 or the Cockroach extension
		// errcode RetriableError:CR000. The Cockroach extension has been removed server-side, but support
		// for it has been left here for now to maintain backwards compatibility.
		pqErr, ok := err.(*pq.Error)
		if retryable := ok && (pqErr.Code == "CR000" || pqErr.Code == "40001"); !retryable {
			if released {
				err = &AmbiguousCommitError{err}
			}
			return err
		}
		if _, err = tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
			return err
		}
	}
}

// AmbiguousCommitError represents an error that left a transaction in an
// ambiguous state: unclear if it committed or not.
type AmbiguousCommitError struct {
	error
}
