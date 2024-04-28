package server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/farouqzaib/fast-search/internal/storage"
	"github.com/gorilla/mux"
	"go.etcd.io/bbolt"
)

func NewHttpServer(index *storage.DistributedDB, metadataStorage *bbolt.DB, logger *slog.Logger, addr string) *http.Server {
	srv := newHttpServer(index, metadataStorage, logger)
	r := mux.NewRouter()
	r.HandleFunc("/search", srv.handleSearch).Methods("GET")
	r.HandleFunc("/index", srv.handleIndex).Methods("POST")
	r.HandleFunc("/join", srv.handleJoin).Methods("POST")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Index           *storage.DistributedDB
	Logger          *slog.Logger
	MetadataStorage *bbolt.DB
}

func newHttpServer(index *storage.DistributedDB, metadataStorage *bbolt.DB, logger *slog.Logger) *httpServer {
	return &httpServer{
		Index:           index,
		Logger:          logger,
		MetadataStorage: metadataStorage,
	}
}

type SearchRequest struct {
	Query string `json:"query"`
}

type Hit struct {
	DocId    int     `json:"documentID"`
	Offset   []int   `json:"offset"`
	Document string  `json:"document"`
	Score    float64 `json:"score"`
}

type SearchResponse struct {
	Hits []Hit `json:"hits"`
}

func (s *httpServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	s.Logger.Info("http: search")

	var req SearchRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.Logger.Info("query term", slog.String("query", req.Query))

	matches, err := s.Index.Search(req.Query, 10)

	if err != nil {
		slog.Error("http: search", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := SearchResponse{}

	err = s.MetadataStorage.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(storage.DocumentMetadataBucket))
		if b == nil {
			return errors.New("bucket does not exist")
		}

		for _, match := range matches {
			hit := Hit{
				DocId:    int(match.Offsets[0].DocumentID),
				Document: string(b.Get(itob(int(match.Offsets[0].DocumentID)))),
				Offset:   []int{},
				Score:    match.Score,
			}

			//only FTS records term offsets
			if len(match.Offsets) == 2 {
				hit.Offset = []int{int(match.Offsets[0].Offset), int(match.Offsets[1].Offset)}
			}

			res.Hits = append(res.Hits, hit)
		}

		return nil
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	return
}

type OkResponse struct {
	Status string `json:"status"`
}

type Document struct {
	Text string `json:"text"`
}

func (s *httpServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	s.Logger.Info("http: indexing")
	var req Document
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)

	if err != nil {
		slog.Error("http: indexing", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var docId int
	err = s.MetadataStorage.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(storage.DocumentMetadataBucket))
		if b == nil {
			return errors.New("bucket does not exist")
		}

		id, _ := b.NextSequence()
		docId = int(id)
		err := b.Put(itob(docId), []byte(req.Text))

		if err != nil {
			slog.Error("http: indexing", slog.String("error", err.Error()))
			return err
		}
		return nil
	})

	if err != nil {
		slog.Error("http: indexing", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	err = s.Index.Index(docId, req.Text)
	if err != nil {
		slog.Error("http: indexing", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(OkResponse{Status: "OK!"})
	if err != nil {
		slog.Error("http: indexing", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	return
}

type JoinRequest struct {
	NodeID string `json:"nodeID"`
	Addr   string `json:"addr"`
}

func (s *httpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	s.Logger.Info("http: cluster join")
	var req JoinRequest
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)

	if err != nil {
		slog.Error("http: cluster join", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.Index.Join(req.NodeID, req.Addr)

	if err != nil {
		slog.Error("http: cluster join", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(OkResponse{Status: "OK!"})
	if err != nil {
		slog.Error("http: cluster join", slog.String("error", err.Error()))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	return
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
