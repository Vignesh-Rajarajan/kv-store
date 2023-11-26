package db_test

import (
	"github.com/Vignesh-Rajarajan/distributed-kv-store/db"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func createTempDb(t *testing.T, readOnly bool) *db.KVDatabase {
	t.Helper()
	f, err := os.CreateTemp(os.TempDir(), "kvdb")
	assert.NoError(t, err)

	name := f.Name()
	assert.NoError(t, f.Close())
	defer func(name string) {
		err := os.Remove(name)
		assert.NoError(t, err)
	}(name)

	kvdb, err := db.NewDatabase(name, false)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, kvdb.Close())
	})
	return kvdb
}

func TestGetSet(t *testing.T) {
	kvdb := createTempDb(t, false)
	err := kvdb.SetKey("key", "value")
	assert.NoError(t, err)
	value, err := kvdb.GetKey("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", value)

	k, v, err := kvdb.GetKeysForReplication()
	assert.NoError(t, err)
	assert.Equal(t, "key", string(k))
	assert.Equal(t, "value", string(v))
}

func TestDeleteReplicationKey(t *testing.T) {
	kvdb := createTempDb(t, false)
	setKey(t, kvdb, "key", "value")

	k, v, err := kvdb.GetKeysForReplication()
	assert.NoError(t, err)
	assert.Equal(t, "key", string(k))
	assert.Equal(t, "value", string(v))

	if err := kvdb.DeleteReplicaKey("key", "value1"); err == nil {
		t.Fatal("key value pair should not exist and hence should throw error")
	}
	if err := kvdb.DeleteReplicaKey("key", "value"); err != nil {
		t.Fatal(err)
	}
	k, v, err = kvdb.GetKeysForReplication()
	assert.NoError(t, err)
	if k != nil || v != nil {
		t.Fatal("key value pair should be deleted")
	}
}

func setKey(t *testing.T, kvdb *db.KVDatabase, key, value string) {
	t.Helper()
	err := kvdb.SetKey(key, value)
	assert.NoError(t, err)
}

func getKey(t *testing.T, kvdb *db.KVDatabase, key string) string {
	t.Helper()
	value, err := kvdb.GetKey(key)
	assert.NoError(t, err)
	return value
}

func TestDeleteUnwantedKeys(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "kvdb")
	assert.NoError(t, err)

	name := f.Name()
	assert.NoError(t, f.Close())
	defer func(name string) {
		err := os.Remove(name)
		assert.NoError(t, err)
	}(name)

	kvdb, err := db.NewDatabase(name, false)
	assert.NoError(t, err)
	defer func() {
		err := kvdb.Close()
		assert.NoError(t, err)
	}()

	setKey(t, kvdb, "key1", "value1")
	setKey(t, kvdb, "key2", "value2")

	if err := kvdb.DeleteUnwantedKeys(func(key string) bool {
		return strings.Contains(key, "1")
	}); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "", getKey(t, kvdb, "key1"))
	assert.Equal(t, "value2", getKey(t, kvdb, "key2"))
}
