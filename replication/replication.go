package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/db"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// NextKeyValue is the struct for the key value pair
type NextKeyValue struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	ErrString error  `json:"err"`
}

type client struct {
	db         *db.KVDatabase
	leaderAddr string
}

func SyncMasterAndReplica(db *db.KVDatabase, leaderAddr string, done chan bool) error {
	c := &client{db: db, leaderAddr: leaderAddr}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			// Signal received, exit the function
			log.Default().Println("done signal received, stopping sync")
			return nil
		case <-ticker.C:
			log.Println("syncing with leader")
			present, err := c.sync()
			if err != nil {
				log.Default().Println("error syncing with leader: ", err)
				continue // Proceed to next iteration of the loop
			}

			if !present {
				log.Default().Println("leader not present")
				continue // Proceed to next iteration of the loop
			}
		}
	}
}

func (c *client) sync() (bool, error) {
	url := "http://" + c.leaderAddr + "/replicate"
	resp, err := http.Get(url)
	if err != nil {
		return false, fmt.Errorf("leader url %s got error %w", url, err)
	}

	var res NextKeyValue

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if res.ErrString != nil {
		return false, fmt.Errorf("%w", err)
	}

	if err := c.db.SetKeyOnReplica(res.Key, res.Value); err != nil {
		return false, err
	}

	if err := c.deleteFromReplicationBuffer(res.Key, res.Value); err != nil {
		log.Default().Println("error deleting key from replication buffer: ", err)
	}
	return true, nil

}

func (c *client) deleteFromReplicationBuffer(key string, value string) error {
	u := url.Values{}
	u.Set("key", key)
	u.Set("value", value)

	log.Printf("Deleting key= %q, value= %q, from replication buffer %q", key, value, c.leaderAddr)
	resp, err := http.Get("http://" + c.leaderAddr + "/deleteReplica" + "?" + u.Encode())
	if err != nil {
		log.Printf("error deleting key from replication buffer: %v", err)
		return err
	}
	defer resp.Body.Close()

	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if !bytes.Equal(res, []byte("ok")) {
		return fmt.Errorf("error deleting key from replication buffer: %s", res)
	}
	return nil

}
