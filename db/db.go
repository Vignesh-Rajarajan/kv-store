package db

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
)

const defaultBucket = "kv"
const replicaBucket = "replica"

// KVDatabase is the database struct
type KVDatabase struct {
	db        *bolt.DB
	closeFunc func() error
	readOnly  bool
}

// NewDatabase creates a new database connection
func NewDatabase(dbLocation string, readOnly bool) (*KVDatabase, error) {
	db, err := bolt.Open(dbLocation, 0600, nil)
	if err != nil {
		return nil, err
	}
	boltDb := &KVDatabase{db: db, closeFunc: db.Close, readOnly: readOnly}

	if err := boltDb.createBuckets(); err != nil {
		_ = boltDb.Close()
		return nil, err
	}
	return boltDb, nil
}

func (db *KVDatabase) createBuckets() error {
	return db.db.Update(func(tx *bolt.Tx) error {

		if _, err := tx.CreateBucketIfNotExists([]byte(defaultBucket)); err != nil {
			return fmt.Errorf("error creating bucket %s: %s", defaultBucket, err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(replicaBucket)); err != nil {
			return fmt.Errorf("error creating bucket %s: %s", replicaBucket, err)
		}
		return nil
	})
}

// Close closes the database connection
func (db *KVDatabase) Close() error {
	fmt.Println("closing db")
	return db.closeFunc()
}

// SetKey sets the key value pair in the database
func (db *KVDatabase) SetKey(key, value string) error {
	if db.readOnly {
		return fmt.Errorf("db is read only")
	}
	return db.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte(defaultBucket)).Put([]byte(key), []byte(value)); err != nil {
			return fmt.Errorf("error writing to bucket %s: %s", defaultBucket, err)
		}
		return tx.Bucket([]byte(replicaBucket)).Put([]byte(key), []byte(value))
	})
}

// GetKey gets the value for the given key
func (db *KVDatabase) GetKey(key string) (string, error) {
	var value string
	err := db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(defaultBucket))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", defaultBucket)
		}
		val := copySlice(bucket.Get([]byte(key)))
		value = string(val)
		return nil
	})
	if err != nil {
		return "", err
	}
	return value, nil
}

func copySlice(s []byte) []byte {
	if s == nil {
		return nil
	}
	b := make([]byte, len(s))
	copy(b, s)
	return b
}

// GetKeysForReplication gets the key value pair for replication
func (d *KVDatabase) GetKeysForReplication() (key, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(replicaBucket))
		k, v := bucket.Cursor().First()
		key = copySlice(k)
		value = copySlice(v)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// DeleteReplicaKey deletes the key value pair from the database
func (d *KVDatabase) DeleteReplicaKey(key, value string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(replicaBucket))
		v := bucket.Get([]byte(key))
		if v == nil {
			return fmt.Errorf("key %s not found", key)
		}
		if string(v) != value {
			return fmt.Errorf("value mismatch for key %s", key)
		}
		return bucket.Delete([]byte(key))
	})
}

// SetKeyOnReplica sets the key value pair in the database
func (db *KVDatabase) SetKeyOnReplica(key, value string) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(defaultBucket)).Put([]byte(key), []byte(value))
	})
}

// DeleteUnwantedKeys deletes the keys that are not present in the current shard
func (db *KVDatabase) DeleteUnwantedKeys(shouldDelete func(key string) bool) error {
	var keysToDelete []string
	err := db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(defaultBucket))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", defaultBucket)
		}
		return bucket.ForEach(func(k, v []byte) error {
			if shouldDelete(string(k)) {
				keysToDelete = append(keysToDelete, string(k))
			}
			return nil
		})
	})
	if err != nil {
		return err
	}

	err = db.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(defaultBucket))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", defaultBucket)
		}
		for _, key := range keysToDelete {
			if err := bucket.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}
