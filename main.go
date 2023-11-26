package main

import (
	"flag"
	kvConf "github.com/Vignesh-Rajarajan/distributed-kv-store/config"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/db"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/replication"
	"github.com/Vignesh-Rajarajan/distributed-kv-store/web"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var (
	dbLocation = flag.String("db-location", "", "database location")
	httpAddr   = flag.String("http-addr", "", "http address")
	configFile = flag.String("config-file", "sharding.toml", "shard config file location")
	shardID    = flag.String("shard", "", "shard id")
	replica    = flag.Bool("replica", false, "read-only replica")
)

// parseFlags parses the command line flags
func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatal("db location is missing")
	}
	if *httpAddr == "" {
		log.Fatal("http address is empty")
	}

	if *shardID == "" {
		log.Fatal("shard id is empty")
	}
}

func main() {

	done := make(chan bool, 1)
	sig := make(chan os.Signal, 1)
	// Notify on specific signals
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	parseFlags()
	log.Println("Starting application with flags:", "db-location:", *dbLocation, "http-addr:", *httpAddr, "config-file:", *configFile, "shard:", *shardID, "replica:", *replica)
	c, err := kvConf.ParseShardConfig(*configFile)
	if err != nil {
		log.Fatal("error parsing config file: ", err)
	}
	shardMeta, err := kvConf.ParseShardMetadata(c.AvailableShard, *shardID)
	if err != nil {
		log.Fatal("error parsing shard metadata: ", err)
	}

	inMemDb, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatal(err)
	}
	if !*replica {
		log.Println("starting replication")
		leaderAddr, ok := shardMeta.Addrs[shardMeta.CurrIdx]
		if !ok {
			log.Fatalf("leader address not found for shard id: %v", shardMeta.CurrIdx)
		}
		log.Println("leader address: ", leaderAddr)

		// Start replication in a separate goroutine
		go replication.SyncMasterAndReplica(inMemDb, leaderAddr, done)
	}
	// Initialize and start the server
	server := web.NewServer(inMemDb, shardMeta)
	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)
	http.HandleFunc("/purge", server.DeleteKeysHandler)
	http.HandleFunc("/replicate", server.ReplicateHandler)
	http.HandleFunc("/deleteReplica", server.DeleteReplicaHandler)

	// Start the server in a separate goroutine
	go func() {
		log.Println("server started on ", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, nil); err != nil {
			log.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()

	// Handle OS signals
	go func() {
		<-sig
		log.Println("kill signal received")
		close(done)
		inMemDb.Close()
	}()

	// Wait for the server or replication goroutine to exit
	select {
	case <-done:
		log.Println("Shutting down due to signal stop")
	}

}
