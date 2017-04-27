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
// Author: Peng Gao (peng.gao.dut@gmail.com)

package crkv

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/hashicorp/go-memdb"
)

type InmemKeyValuePair struct {
	Key   string
	Value Value
}

//go:generate safemap -k string -v Value -n inmem

// InmemKVStore is based on github.com/ggaaooppeenngg/safemap
// key contains byte slice, it can not be a key in map, so
// use string instead as a key.
type InmemKVStore struct {
	table string
	*memdb.MemDB
}

func NewInmemKVStore() Store {
	// Create the DB schema
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"kvt": &memdb.TableSchema{ // default table is kvt
				Name: "kvt",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Key"},
					},
				},
			},
		},
	}
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}
	return &InmemKVStore{
		table: "kvt",
		MemDB: db,
	}
}

// Get retrieves the value for a key.
func (kv *InmemKVStore) Get(_ context.Context, key interface{}) (Value, error) {
	kb, err := i2Bytes(key)
	if err != nil {
		return Value{}, nil
	}
	// read trasaction has no commit or abort
	tx := kv.MemDB.Txn(false)
	value, err := tx.First(kv.table, "id", string(kb))
	if err != nil {
		return Value{}, err
	}
	if value == nil {
		return Value{}, errors.New("not exists")
	}
	return value.(InmemKeyValuePair).Value, nil
}

// Put sets a value for a key.
func (kv *InmemKVStore) Put(_ context.Context, key interface{}, value interface{}) error {
	kb, vb := []byte{}, []byte{}
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	vb, err = i2Bytes(value)
	if err != nil {
		return err
	}
	tx := kv.MemDB.Txn(true)
	err = tx.Insert(kv.table, InmemKeyValuePair{
		Key:   string(kb),
		Value: Value{RawBytes: vb},
	})
	if err != nil {
		tx.Abort()
	} else {
		tx.Commit()
	}
	return err
}

// Del deletes a value for a key.
func (kv *InmemKVStore) Del(ctx context.Context, key interface{}) error {
	kb, err := i2Bytes(key)
	if err != nil {
		return err
	}
	tx := kv.MemDB.Txn(true)
	err = tx.Delete(kv.table, InmemKeyValuePair{
		Key: string(kb),
	})
	if err != nil {
		tx.Abort()
	} else {
		tx.Commit()
	}
	return err
}

// Scan retrieves the key/value pairs between begin
// (inclusive) and end (exclusive) in ascending order.
func (kv *InmemKVStore) Scan(ctx context.Context, begin, end interface{}, max int64) ([]KeyValuePair, error) {
	bb, err := i2Bytes(begin)
	if err != nil {
		return nil, err
	}
	eb, err := i2Bytes(end)
	if err != nil {
		return nil, err
	}
	tx := kv.MemDB.Txn(false)
	it, err := tx.Get(kv.table, "id")
	if err != nil {
		return nil, err
	}
	var retKvps []KeyValuePair
	for i := it.Next(); i != nil; i = it.Next() {
		ikvp := i.(InmemKeyValuePair)
		if bytes.Compare(bb, []byte(ikvp.Key)) <= 0 && bytes.Compare([]byte(ikvp.Key), eb) < 0 &&
			len(retKvps) < int(max) {
			retKvps = append(retKvps, KeyValuePair{
				Key: Key{
					RawBytes: []byte(ikvp.Key),
				},
				Value: ikvp.Value,
			})
		}
	}
	return retKvps, nil
}

// It is not pure trasaction currently, just locked, if any error happens, no rollback will be taken.
func (kv *InmemKVStore) Txn(ctx context.Context, fn func(ctx context.Context, txn Txn) error) error {
	tx := kv.MemDB.Txn(true)
	err := fn(ctx, &InmemTxn{
		table: kv.table,
		Txn:   tx,
	})
	if err != nil {
		tx.Abort()
	} else {
		tx.Commit()
	}
	return err
}

// By is the type of a "less" function that defines the ordering of its KeyValuePair arguments.
type By func(kvp1, kvp2 KeyValuePair) bool

func (by By) Sort(kvps []KeyValuePair) {
	sorter := &kvpSorter{
		kvps: kvps,
		by:   by,
	}
	sort.Sort(sorter)
}

// kvpSort joins a By function and a slice of KeyValuPair to be sorted.
type kvpSorter struct {
	kvps []KeyValuePair
	by   func(kvp1, kvp2 KeyValuePair) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (s *kvpSorter) Len() int {
	return len(s.kvps)
}

// Swap is part of sort.Interface.
func (s *kvpSorter) Swap(i, j int) {
	s.kvps[i], s.kvps[j] = s.kvps[j], s.kvps[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *kvpSorter) Less(i, j int) bool {
	return s.by(s.kvps[i], s.kvps[j])
}
