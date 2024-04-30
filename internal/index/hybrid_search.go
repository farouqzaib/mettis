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
func (hs *HybridSearch) Search(query string, k int) []Match {
	ftsResult := hs.FTS.RankProximity(query, k)

	vector, err := hs.getEmbedding(query)
	if err != nil {
		panic(err)
	}
	semanticResult := hs.Semantic.Search(VectorNode{Vector: vector}, 64)

	return mergeResult(IndexResults{FTS: ftsResult, Semantic: semanticResult}, 0.8, k)
}

func mergeResult(results IndexResults, mergeWeight float32, k int) []Match {
	mergedResults := []Match{}

	seen := map[int]bool{}
	for _, r := range results.FTS {
		mergedResults = append(mergedResults, Match{Offsets: r.Offsets, Score: r.Score * float64(1-mergeWeight)})
		seen[int(r.Offsets[0].DocumentID)] = true
	}

	for _, r := range results.Semantic {
		modifiedScore := 1. / math.Exp(r.Score) * float64(mergeWeight)
		if seen[int(r.Offsets[0].DocumentID)] {
			continue
		}
		mergedResults = append(mergedResults, Match{Offsets: r.Offsets, Score: modifiedScore})
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
