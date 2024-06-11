package index

import (
	"reflect"
	"testing"
)

func TestHybridSearchReciprocalRank(t *testing.T) {

	searchResults := IndexResults{
		FTS: []Match{
			{Offsets: []Position{{DocumentID: 1, Offset: 2}, {DocumentID: 1, Offset: 3}}, Score: 0.20},
			{Offsets: []Position{{DocumentID: 2, Offset: 3}, {DocumentID: 2, Offset: 4}}, Score: 0.50},
			{Offsets: []Position{{DocumentID: 3, Offset: 5}, {DocumentID: 3, Offset: 6}}, Score: 0.20},
		},
		Semantic: []Match{
			{Offsets: []Position{{DocumentID: 1, Offset: 0}, {DocumentID: 1, Offset: 0}}, Score: 0.80},
			{Offsets: []Position{{DocumentID: 4, Offset: 0}, {DocumentID: 4, Offset: 0}}, Score: 0.61},
			{Offsets: []Position{{DocumentID: 9, Offset: 0}, {DocumentID: 9, Offset: 0}}, Score: 0.01},
		},
	}

	expected := []Match{
		{Offsets: []Position{{DocumentID: 1, Offset: 2}, {DocumentID: 1, Offset: 3}}, Score: 2.1},
		{Offsets: []Position{{DocumentID: 2, Offset: 3}, {DocumentID: 2, Offset: 4}}, Score: 0.55},
	}

	got := mergeResult(searchResults, 2)

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
}
