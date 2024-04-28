package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/farouqzaib/fast-search/internal/server"
	"github.com/farouqzaib/fast-search/internal/storage"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	joinAddr string
	raftAddr string
	httpAddr string
	nodeId   string
)

func main() {
	flag.StringVar(&httpAddr, "httpAddr", "", "address for http servic")
	flag.StringVar(&joinAddr, "joinAddr", "", "address of primary node to join")
	flag.StringVar(&nodeId, "nodeId", "", "node ID")
	flag.StringVar(&raftAddr, "raftAddr", "", "raft address for node")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	config := storage.Config{}
	config.Raft.LocalID = raft.ServerID(nodeId)
	config.Addr = raftAddr
	config.RaftDir = "internal/storage/raft"

	if joinAddr == "" {
		config.Raft.Bootstrap = true
	}

	indexStorage, err := storage.NewDistributedDB("internal/storage/data/indexes", config)

	if err != nil {
		log.Fatal(err)
	}

	metadataStorage, err := bolt.Open("internal/storage/data/metadata", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	metadataStorage.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(storage.DocumentMetadataBucket))
		if err != nil {
			log.Fatal(err)
		}
		return nil
	})

	defer metadataStorage.Close()

	srv := server.NewHttpServer(indexStorage, metadataStorage, logger, httpAddr)
	logger.Info("starting server")

	signalCh := make(chan os.Signal, 1)

	go func() {
		log.Fatal(srv.ListenAndServe())
	}()

	signal.Notify(
		signalCh,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
	)

	<-signalCh
	slog.Info("shutdown: flushing memtables to disk")
	indexStorage.DB.FlushMemtables()

}
