package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestDistributedDB(t *testing.T) {
	var dbs []*DistributedDB
	nodeCount := 3
	ports := []int{9000, 9001, 9002}

	for i := 0; i < nodeCount; i++ {
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		fmt.Println(dataDir)

		config := Config{}
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		// config.Raft.HeartbeatTimeout = 100 * time.Millisecond
		// config.Raft.ElectionTimeout = 100 * time.Millisecond
		// config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		// config.Raft.CommitTimeout = 5 * time.Millisecond
		config.Addr = fmt.Sprintf("127.0.0.1:%d", ports[i])
		config.RaftDir = dataDir

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		l, err := NewDistributedDB(dataDir, config)
		require.NoError(t, err)

		if i != 0 {
			err = dbs[0].Join(
				fmt.Sprintf("%d", i), fmt.Sprintf("127.0.0.1:%d", ports[i]),
			)
			fmt.Println("Follower join error:", err)
		} else {
			err = l.WaitForLeader(5 * time.Second)
			require.NoError(t, err)
		}

		dbs = append(dbs, l)
	}

	documents := map[int]string{1: "still works", 8: "raft can be so much fun!"}

	for k, v := range documents {
		err := dbs[0].Index(k, v)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		for j := 0; j < nodeCount; j++ {
			got, err := dbs[j].Search("raft", 10)
			fmt.Println(got, err)
		}
		return true
	}, 5*time.Second, 1*time.Second)
}
