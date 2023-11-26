package config_test

import (
	"github.com/Vignesh-Rajarajan/distributed-kv-store/config"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestParseShardConfig(t *testing.T) {
	contents := `[[shard]]
                 name = "shard1"
                 address = "localhost:8080"
				 shardId = 1`
	f, err := os.CreateTemp(os.TempDir(), "sharding.toml")
	if err != nil {
		t.Fatal(err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(f)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			t.Fatal(err)
		}
	}(f.Name())

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatal(err)
	}

	c, err := config.ParseShardConfig(f.Name())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c.AvailableShard))
}
