package web

import (
	"encoding/json"
	"fmt"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/config"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/db"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/replication"
	"io"
	"log"
	"net/http"
)

type Server struct {
	db            *db.KVDatabase
	shardMetadata *config.ShardMetadata
}

func NewServer(db *db.KVDatabase, s *config.ShardMetadata) *Server {
	return &Server{
		db:            db,
		shardMetadata: s,
	}
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	log.Printf("Redirecting request to shard %d", shard)
	for k, v := range s.shardMetadata.Addrs {
		log.Printf("Key %d, val %s", k, v)
	}
	url := "http://" + s.shardMetadata.Addrs[shard] + r.RequestURI
	log.Println("Redirecting to ", url)
	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Println(err)
		return
	}
	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("SET request received")
	_ = r.ParseForm()

	key := r.Form.Get("key")
	value := r.Form.Get("value")
	if key == "" || value == "" {
		log.Println("key or value is empty")
		return
	}

	shard := s.shardMetadata.GetShard(key)
	if shard != s.shardMetadata.CurrIdx {
		log.Println(fmt.Sprintf("Redirecting to shard %d", shard))
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Println(err)
		return
	}
	log.Println(fmt.Sprintf("Shard = %d, current shard = %d, addr = %q, Value = %q, error = %v", shard, s.shardMetadata.CurrIdx, s.shardMetadata.Addrs[shard], value, err))
}

func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("GET request received")
	_ = r.ParseForm()

	key := r.Form.Get("key")
	if key == "" {
		log.Println("key is empty")
		return
	}
	shard := s.shardMetadata.GetShard(key)
	if shard != s.shardMetadata.CurrIdx {
		s.redirect(shard, w, r)
		return
	}
	value, err := s.db.GetKey(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Println(err)
		return
	}

	_, err = w.Write([]byte(value))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	log.Println(fmt.Sprintf("Shard = %d, current shard = %d, addr = %q, Value = %q, error = %v", shard, s.shardMetadata.CurrIdx, s.shardMetadata.Addrs[shard], value, err))
}

func (s *Server) DeleteKeysHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("DELETE request received")

	fmt.Fprintf(w, "Error = %v", s.db.DeleteUnwantedKeys(func(key string) bool {
		shard := s.shardMetadata.GetShard(key)
		return shard != s.shardMetadata.CurrIdx
	}))
}

func (s *Server) ReplicateHandler(writer http.ResponseWriter, request *http.Request) {
	enc := json.NewEncoder(writer)
	k, v, err := s.db.GetKeysForReplication()
	kv := &replication.NextKeyValue{
		Key:   string(k),
		Value: string(v),
	}
	if err != nil {
		kv.ErrString = fmt.Errorf("error getting key value pair for replication: %w", err)
	}
	enc.Encode(kv)

}

func (s *Server) DeleteReplicaHandler(writer http.ResponseWriter, request *http.Request) {
	_ = request.ParseForm()
	key := request.Form.Get("key")
	value := request.Form.Get("value")
	if key == "" || value == "" {
		log.Println("key or value is empty")
		return
	}
	err := s.db.DeleteReplicaKey(key, value)
	if err != nil {
		log.Println("error deleting key from replica: ", err)
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
		log.Println(err)
		return
	}
	fmt.Fprintf(writer, "ok")
}
