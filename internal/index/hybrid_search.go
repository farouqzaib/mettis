package index

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"sort"
)

type TextEmbeddingResponse struct {
	Status string    `json:"status"`
	Data   []float64 `json:"data"`
}

type TextEmbeddingRequest struct {
	Text string `json:"text"`
}

type IndexResults struct {
	FTS      []Match
	Semantic []Match
}

type HybridSearch struct {
	FTS          *InvertedIndex
	Semantic     *HNSW
	logger       *slog.Logger
	getEmbedding getEmbeddingFn
}

func NewHybridSearch(fts *InvertedIndex, semantic *HNSW, logger *slog.Logger, getEmbedding getEmbeddingFn) *HybridSearch {
	return &HybridSearch{
		FTS:          fts,
		Semantic:     semantic,
		getEmbedding: getEmbedding,
	}
}

func (hs *HybridSearch) Index(docId int, document string) error {
	vector, err := hs.getEmbedding(document)
	if err != nil {
		return err
	}

	hs.FTS.Index(docId, document)
	hs.Semantic.Create([]VectorNode{{Vector: vector, ID: docId}})

	return nil
}

func (hs *HybridSearch) BulkIndex(docIds []float64, documents []string) error {
	jobsCh := make(chan map[int]string, len(docIds))
	resultsCh := make(chan int, len(docIds))

	//TODO: make number of workers configurable
	for worker := 0; worker < 8; worker++ {
		slog.Info("bulk indexing: worker", slog.Int("worker", worker))
		go func(jobs chan map[int]string) {
			for job := range jobs {
				for docId, document := range job {
					vector, err := hs.getEmbedding(document)
					if err != nil {
						slog.Error("bulk indexing error:", err)
						panic(err)
					}

					hs.FTS.Index(docId, document)
					hs.Semantic.Create([]VectorNode{{Vector: vector, ID: docId}})

					resultsCh <- 1
				}
			}
		}(jobsCh)
	}

	//send tasks to goroutines
	for i := 0; i < len(docIds); i++ {
		jobsCh <- map[int]string{int(docIds[i]): documents[i]}
	}

	//process results
	for k := 0; k < len(docIds); k++ {
		<-resultsCh
	}
	return nil
}

func (hs *HybridSearch) Search(query string, k int) ([]Match, error) {
	ftsResult := hs.FTS.RankProximity(query, k)

	vector, err := hs.getEmbedding(query)
	if err != nil {
		return []Match{}, err
	}
	semanticResult := hs.Semantic.Search(VectorNode{Vector: vector}, 64)

	return mergeResult(IndexResults{FTS: ftsResult, Semantic: semanticResult}, k), nil
}

func mergeResult(results IndexResults, k int) []Match {
	mergedResults := []Match{}

	seen := map[string]Match{}
	for rank, r := range results.FTS {
		matchID, _ := r.GetKey()
		reciprocalRank := 1.1 / (float64(rank) + 1.)
		seen[matchID] = Match{Offsets: r.Offsets, Score: reciprocalRank}
	}

	for rank, r := range results.Semantic {
		matchID, _ := r.GetKey()
		reciprocalRank := 1. / (float64(rank) + 1.)

		val, ok := seen[matchID]
		if ok {
			val.Score += reciprocalRank
			seen[matchID] = val
		} else {
			seen[matchID] = Match{Offsets: r.Offsets, Score: reciprocalRank}
		}
	}

	for _, v := range seen {
		mergedResults = append(mergedResults, v)
	}

	sort.Slice(mergedResults, func(i, j int) bool {
		return mergedResults[i].Score > mergedResults[j].Score
	})

	k = int(math.Min(float64(k), float64(len(mergedResults))))
	return mergedResults[:k]
}

type getEmbeddingFn func(text string) ([]float64, error)

func GetEmbedding(text string) ([]float64, error) {
	embeddingHost := os.Getenv("EmbeddingHost")
	postBody, _ := json.Marshal(map[string]string{
		"text": text,
	})
	responseBody := bytes.NewBuffer(postBody)
	resp, err := http.Post(embeddingHost, "application/json", responseBody)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var embedding TextEmbeddingResponse

	err = json.Unmarshal(body, &embedding)
	if err != nil {
		return nil, err
	}

	return embedding.Data, nil
}
