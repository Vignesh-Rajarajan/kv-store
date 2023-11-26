package web_test

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/config"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/db"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/web"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func createShardDb(t *testing.T, id int) *db.KVDatabase {
	f, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("kvdb-%d", id))
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

func createShardServer(t *testing.T, id int, addrs map[int]string) (*db.KVDatabase, *web.Server) {
	t.Helper()
	kvdb := createShardDb(t, id)
	shardMeta := &config.ShardMetadata{
		Count:   len(addrs),
		CurrIdx: id,
		Addrs:   addrs,
	}
	server := web.NewServer(kvdb, shardMeta)
	return kvdb, server
}

func TestWebServer(t *testing.T) {
	var test1GetHandler, test1SetHandler func(w http.ResponseWriter, r *http.Request)
	var test2GetHandler, test2SetHandler func(w http.ResponseWriter, r *http.Request)

	testServer1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get":
			test1GetHandler(w, r)
		case "/set":
			test1SetHandler(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer testServer1.Close()

	testServer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get":
			test2GetHandler(w, r)
		case "/set":
			test2SetHandler(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer testServer2.Close()

	addrs := map[int]string{
		0: strings.TrimPrefix(testServer1.URL, "http://"),
		1: strings.TrimPrefix(testServer2.URL, "http://"),
	}

	kvdb1, server1 := createShardServer(t, 0, addrs)
	kvdb2, server2 := createShardServer(t, 1, addrs)

	keys := map[string]int{
		"INDIAfsdfsfs": 1,
		"USA":          0,
	}

	test1GetHandler = server1.GetHandler
	test1SetHandler = server1.SetHandler
	test2GetHandler = server2.GetHandler
	test2SetHandler = server2.SetHandler

	for key := range keys {
		_, err := http.Get(fmt.Sprintf(testServer1.URL+"/set?key=%s&value=value-%s", key, key))
		assert.NoError(t, err)
	}

	for key := range keys {
		resp, err := http.Get(fmt.Sprintf(testServer1.URL+"/get?key=%s", key))
		assert.NoError(t, err)
		assert.Equal(t, resp.StatusCode, http.StatusOK)

		contents, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%s", key), string(contents))
		log.Default().Println(string(contents))
	}

	value1, err := kvdb1.GetKey("USA")
	assert.NoError(t, err)
	assert.Equal(t, "value-USA", value1)

	value2, err := kvdb2.GetKey("INDIAfsdfsfs")
	assert.NoError(t, err)
	assert.Equal(t, "value-INDIAfsdfsfs", value2)
}
