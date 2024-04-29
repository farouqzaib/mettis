package index

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/farouqzaib/fast-search/internal/analyzer"
)

func TestInvertedIndexIndex(t *testing.T) {
	index := NewInvertedIndex(slog.Default())

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := DocumentOffset{DocumentID: 1, Offset: 2}

	sk := index.PostingsList["name"]
	found, err := sk.Find(expected)

	if err != nil {
		t.Fatalf("expected %v, document offset, got %v", expected, found)
	}
}

func TestInvertedIndexPrevious(t *testing.T) {
	index := NewInvertedIndex(slog.Default())

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := DocumentOffset{DocumentID: 1, Offset: 2}

	got, _ := index.Previous("name", DocumentOffset{DocumentID: 1, Offset: 3})

	if expected.DocumentID != got.DocumentID {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}

func TestInvertedIndexNext(t *testing.T) {
	index := NewInvertedIndex(slog.Default())

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := DocumentOffset{DocumentID: EOF, Offset: EOF}

	got, _ := index.Next("my", DocumentOffset{DocumentID: 1, Offset: 1})

	fmt.Println(got)
	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestInvertedIndexNextPhrase(t *testing.T) {
	index := NewInvertedIndex(slog.Default())

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := []DocumentOffset{{DocumentID: 3, Offset: 2}, {DocumentID: 3, Offset: 3}}

	got := index.NextPhrase("your name", DocumentOffset{DocumentID: BOF, Offset: BOF})

	fmt.Println(got)
	if len(got) != 2 {
		t.Fatalf("expected 2 document offsets, got %v", len(got))
	}

	if expected[1].Offset-expected[0].Offset != 1 {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}

func TestInvertedIndexNextCover(t *testing.T) {
	index := NewInvertedIndex(slog.Default())

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")

	expected := []DocumentOffset{{DocumentID: 1, Offset: 1}, {DocumentID: 1, Offset: 2}}

	tokens := analyzer.Analyze("my batman")
	got := index.NextCover(tokens, DocumentOffset{DocumentID: BOF, Offset: BOF})

	b := index.Encode()
	i := index.Decode(b)

	got = i.NextCover(tokens, DocumentOffset{DocumentID: BOF, Offset: BOF})

	fmt.Println(got)
	if len(got) != 2 {
		t.Fatalf("expected 2 document offsets, got %v", len(got))
	}

	if got[1].Offset-got[0].Offset != 1 {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}

func TestInvertedIndexRankProximity(t *testing.T) {
	index := NewInvertedIndex(slog.Default())

	index.Index(1, "hello, my name is BATMAN!")
	index.Index(2, "I have come to save Gotham!")
	index.Index(3, "What is your name")
	index.Index(4, "What is my your name")

	expected := []DocumentOffset{{DocumentID: 3, Offset: 2}, {DocumentID: 3, Offset: 3}}

	got := index.RankProximity("save my gotham", 10)

	if expected[1].Offset-expected[0].Offset != 1 {
		t.Fatalf("expected %v, document offset, got %v", expected, got)
	}
}
