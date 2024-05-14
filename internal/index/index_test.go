package index

import (
	"fmt"
	"testing"

	"github.com/farouqzaib/fast-search/internal/analyzer"
)

func TestInvertedIndexIndex(t *testing.T) {
	index := NewInvertedIndex()

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")
	index.Index(4, "Where in Gotham is the Joker?")

	expected := Position{DocumentID: 4, Offset: 0}

	sk := index.PostingsList["gotham"]

	got, err := sk.FindGreaterThan(Position{DocumentID: 2, Offset: 0})

	if err != nil {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}

func TestInvertedIndexPrevious(t *testing.T) {
	index := NewInvertedIndex()

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")
	index.Index(4, "Where in Gotham is the Joker?")

	expected := Position{DocumentID: 2, Offset: 0}

	got, _ := index.Previous("gotham", Position{DocumentID: 4, Offset: 0})

	if expected.DocumentID != got.DocumentID {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}

func TestInvertedIndexNext(t *testing.T) {
	index := NewInvertedIndex()

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := Position{DocumentID: EOF, Offset: EOF}

	got, _ := index.Next("my", Position{DocumentID: 1, Offset: 1})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestInvertedIndexNextPhrase(t *testing.T) {
	index := NewInvertedIndex()

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := []Position{{DocumentID: 3, Offset: 2}, {DocumentID: 3, Offset: 3}}

	got := index.NextPhrase("your name", Position{DocumentID: BOF, Offset: BOF})

	if len(got) != 2 {
		t.Fatalf("expected 2 document offsets, got %v", len(got))
	}

	if expected[1].Offset-expected[0].Offset != 1 {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}

func TestInvertedIndexNextCover(t *testing.T) {
	index := NewInvertedIndex()

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := []Position{{DocumentID: 1, Offset: 1}, {DocumentID: 1, Offset: 1}}

	tokens := analyzer.Analyze("my batman")

	b, err := index.Encode()

	if err != nil {
		t.Fatalf("index encode returned an error")
	}

	var reloadedIndex InvertedIndex
	reloadedIndex.Decode(b)

	got := reloadedIndex.NextCover(tokens, Position{DocumentID: BOF, Offset: BOF})

	fmt.Println(got)
	if len(got) != 2 {
		t.Fatalf("expected 2 document offsets, got %v", len(got))
	}

	if got[1].Offset != got[0].Offset {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}

func TestInvertedIndexRankProximity(t *testing.T) {
	index := NewInvertedIndex()

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")
	index.Index(4, "What is my your name")

	expected := []Position{{DocumentID: 3, Offset: 2}, {DocumentID: 3, Offset: 3}}

	got := index.RankProximity("save my gotham", 10)

	if expected[1].Offset-expected[0].Offset != 1 {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}
