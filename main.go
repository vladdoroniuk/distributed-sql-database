package main

import (
	// "bytes"
	// "encoding/json"
	// "fmt"
	// "io"
	// "log"
	// "net"
	// "net/http"
	// "os"
	// "path"
	// "strings"
	// "time"

	// "github.com/google/uuid"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	// "github.com/hashicorp/raft-boltdb"
	// "github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	// bolt "go.etcd.io/bbolt"
)

type pgFsm struct {
	pe *pgEngine
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(sink raft.SnapshotSink) error {
	return sink.Cancel()
}

func (sn snapshotNoop) Release() {}

func (pf *pgFsm) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (pf *pgFsm) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("Nothing to restore")
}

func (pf *pgFsm) Apply(log *raft.Log) {
	switch log.Type {
	case raft.LogCommand:
		ast, err := pgquery.Parse(string(log.Data))
		if err != nil {
			panic(fmt.Errorf("Couldn't parse payload: %s", err))
		}

		err = pf.pe.execute(ast)
		if err != nil {
			panic(err)
		}
	default:
		panic(fmt.Errorf("Unknown raft log type: ", log.Type))
	}
}
