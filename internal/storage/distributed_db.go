package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/farouqzaib/fast-search/internal/index"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type DistributedDB struct {
	DB     *IndexStorage
	raft   *raft.Raft
	config Config
	logger *slog.Logger
}

func NewDistributedDB(dataDir string, config Config, logger *slog.Logger) (*DistributedDB, error) {
	d := &DistributedDB{}
	d.config = config
	d.logger = logger

	if err := d.setupIndex(dataDir); err != nil {
		return nil, err
	}

	if err := d.setupRaft(config.RaftDir); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DistributedDB) setupIndex(dataDir string) error {
	db, err := Open(dataDir, d.logger)
	if err != nil {
		return err
	}

	d.DB = db

	return err
}

func (d *DistributedDB) setupRaft(dataDir string) error {
	fsm := &fsm{db: d.DB}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	logStore = boltDB
	stableStore = boltDB

	if err != nil {
		return err
	}

	retain := 1

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), retain, os.Stderr)

	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second

	addr, err := net.ResolveTCPAddr("tcp", d.config.Addr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(d.config.Addr, addr, maxPool, timeout, os.Stderr)
	if err != nil {
		return err
	}

	config := raft.DefaultConfig()
	config.LocalID = d.config.Raft.LocalID

	if d.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = d.config.Raft.HeartbeatTimeout
	}

	if d.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = d.config.Raft.ElectionTimeout
	}

	if d.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = d.config.Raft.LeaderLeaseTimeout
	}

	if d.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = d.config.Raft.CommitTimeout
	}

	d.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)

	if err != nil {
		return err
	}

	if d.config.Raft.Bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}

		err = d.raft.BootstrapCluster(config).Error()
	}

	return err

}

type Config struct {
	Raft struct {
		raft.Config
		StreamLayer *raft.StreamLayer
		Bootstrap   bool
	}
	Addr    string
	RaftDir string
}

func (d *DistributedDB) Index(docId int, document string) error {
	c := &command{
		Op:   "index",
		Data: map[string]interface{}{"docId": docId, "document": document},
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	timeout := 10 * time.Second
	future := d.raft.Apply(b, timeout)

	if future.Error() != nil {
		return future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return err
	}

	return nil
}

func (d *DistributedDB) BulkIndex(docIds []int, documents []string) error {
	c := &command{
		Op:   "bulkIndex",
		Data: map[string]interface{}{"docIds": docIds, "documents": documents},
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	timeout := 10 * time.Second
	future := d.raft.Apply(b, timeout)

	if future.Error() != nil {
		return future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return err
	}

	return nil
}

func (d *DistributedDB) Search(query string, k int) ([]index.Match, error) {
	res := d.DB.Get(query, 10)

	return res, nil
}

func (d *DistributedDB) Join(nodeID, addr string) error {
	configFuture := d.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		d.logger.Error("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				d.logger.Info(fmt.Sprintf("node %s at %s already member of cluster, ignoring join request", nodeID, addr))
				return nil
			}

			future := d.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := d.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	d.logger.Info(fmt.Sprintf("node %s at %s joined successfully", nodeID, addr))
	return nil
}

func (d *DistributedDB) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := d.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	db *IndexStorage
}

type command struct {
	Op   string                 `json:"op,omitempty"`
	Data map[string]interface{} `json:"data,omitempty"`
}

func (f *fsm) Apply(b *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(b.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "index":
		docId := int(c.Data["docId"].(float64))
		document := c.Data["document"].(string)
		return f.applyIndex(docId, document)
	case "search":
		query := c.Data["query"].(string)
		return f.applySearch(query)
	case "bulkIndex":
		rawDocIds := c.Data["docIds"].([]interface{})

		docIds := []float64{}

		for _, d := range rawDocIds {
			docIds = append(docIds, d.(float64))
		}

		documents := []string{}
		rawDocuments := c.Data["documents"].([]interface{})
		for _, d := range rawDocuments {
			documents = append(documents, d.(string))
		}
		return f.applyBulkIndex(docIds, documents)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

func (f *fsm) applyBulkIndex(docIds []float64, documents []string) interface{} {
	err := f.db.BulkIndex(docIds, documents)
	if err != nil {
		return err
	}

	return nil
}

func (f *fsm) applyIndex(docId int, document string) interface{} {
	err := f.db.Index(docId, document)
	if err != nil {
		return err
	}

	return nil
}

func (f *fsm) applySearch(query string) interface{} {
	res := f.db.Get(query, 10)

	return res
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.db.Reader()
	return &snapshot{invertedIndexReader: r[0], vectorIndexReader: r[1]}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error { return nil }

type snapshot struct {
	invertedIndexReader io.Reader
	vectorIndexReader   io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.vectorIndexReader); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (f *snapshot) Release() {}
