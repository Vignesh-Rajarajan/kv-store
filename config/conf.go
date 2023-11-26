package config

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"hash/fnv"
)

// Shard contains the config of the shard
type Shard struct {
	ShardId int    `toml:"shardId"`
	Name    string `toml:"name"`
	Address string `toml:"address"`
}

// ShardConfig contains the config of the shards
type ShardConfig struct {
	AvailableShard []Shard `toml:"shard"`
}

// ParseShardConfig parses the shard config file
func ParseShardConfig(file string) (ShardConfig, error) {
	var c ShardConfig
	if _, err := toml.DecodeFile(file, &c); err != nil {
		return c, err
	}
	return c, nil
}

// ShardMetadata contains the metadata of the shards
type ShardMetadata struct {
	Count   int
	CurrIdx int
	Addrs   map[int]string
}

// ParseShardMetadata parses the shard metadata
func ParseShardMetadata(shards []Shard, currShardName string) (*ShardMetadata, error) {

	shardCount := len(shards)
	shardIdx := -1
	addrShardPair := make(map[int]string)

	for _, shard := range shards {
		if _, ok := addrShardPair[shard.ShardId]; ok {
			return nil, fmt.Errorf("duplicate shard id %d", shard.ShardId)
		}
		addrShardPair[shard.ShardId] = shard.Address
		if shard.Name == currShardName {
			shardIdx = shard.ShardId
		}
	}
	if shardIdx < 0 {
		return nil, fmt.Errorf("shard id %q not found", currShardName)
	}

	return &ShardMetadata{
		Count:   shardCount,
		CurrIdx: shardIdx,
		Addrs:   addrShardPair,
	}, nil
}

// GetShardId returns the shard id for the given key
func (s *ShardConfig) GetShardId(name string) int {
	for _, shard := range s.AvailableShard {
		if shard.Name == name {
			return shard.ShardId
		}
	}
	return -1
}

// GetAddrMapping returns the mapping of shard id to address
func (s *ShardConfig) GetAddrMapping() map[int]string {
	addrMapping := make(map[int]string)
	for _, shard := range s.AvailableShard {
		addrMapping[shard.ShardId] = shard.Address
	}
	return addrMapping
}

// GetShard returns the shard id for the given key
func (s *ShardMetadata) GetShard(key string) int {
	hash := fnv.New64()
	_, err := hash.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(hash.Sum64() % uint64(s.Count))
}
