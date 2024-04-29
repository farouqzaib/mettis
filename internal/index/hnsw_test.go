package index

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func randomPoint() VectorNode {
	var v = make([]float64, 16)
	for i := range v {
		v[i] = rand.Float64()
	}
	return VectorNode{Vector: v}
}

func TestHNSW(t *testing.T) {
	vectors := []VectorNode{}

	for i := 1; i <= 10000; i++ {
		vectors = append(vectors, randomPoint())

		if (i)%1000 == 0 {
			fmt.Printf("%v points added\n", i)
		}
	}

	hnsw := NewHNSW(5, 0.62, 2, 10)

	hnsw.Create(vectors)

	start := time.Now()
	for i := 0; i < 1000; i++ {
		hnsw.Search(randomPoint(), 10)
	}
	stop := time.Since(start)

	fmt.Printf("%v queries / second (single thread)\n", 1000.0/stop.Seconds())
	fmt.Printf("%+v", hnsw.Search(randomPoint(), 10))

	// var b bytes.Buffer
	// enc := gob.NewEncoder(&b)

	// err := enc.Encode(hnsw)
	// if err != nil {
	// 	log.Fatal("encode error:", err)
	// }
	// fmt.Println("Length of bytes:", len(b.Bytes()))

	// var q HNSW
	// dec := gob.NewDecoder(&b)
	// err = dec.Decode(&q)
	// if err != nil {
	// 	log.Fatal("decode error 1:", err)
	// }
	// fmt.Println("decoded index:", q.Search(randomPoint(), 10))
}
