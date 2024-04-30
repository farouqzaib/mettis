package index

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"math"
	"math/rand"
)

type maxHeap []Candidate

func (h maxHeap) Len() int { return len(h) }
func (h maxHeap) Less(i, j int) bool {
	return h[i].Distance > h[j].Distance
}
func (h maxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *maxHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Candidate))
}

func (h *maxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type minHeap []Candidate

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(i, j int) bool {
	return h[i].Distance < h[j].Distance
}
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Candidate))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type VectorNode struct {
	Vector  []float64
	ID      int
	Indices []int
	Entry   int
}

type Graph struct {
	Elements []VectorNode
}

type Candidate struct {
	Distance float64
	Entry    int
}

type HNSW struct {
	L     int
	mL    float64
	M     int
	EFC   int
	Index []Graph
}

func NewHNSW(L int, mL float64, m int, efc int) *HNSW {
	index := make([]Graph, L)

	return &HNSW{
		L:     L,
		mL:    mL,
		M:     m,
		EFC:   efc,
		Index: index,
	}
}

func (hnsw *HNSW) searchLayer(graph Graph, entry int, query VectorNode, ef int) []Candidate {
	candidate := Candidate{distance(query.Vector, graph.Elements[entry].Vector), entry}

	nearestNeighbours := &maxHeap{candidate}
	heap.Init(nearestNeighbours)

	visited := make(map[int]map[float64]bool)
	visited[candidate.Entry] = map[float64]bool{candidate.Distance: true}

	candidateHeap := &minHeap{candidate}
	heap.Init(candidateHeap)

	for candidateHeap.Len() > 0 {
		current := heap.Pop(candidateHeap).(Candidate)

		if current.Distance > (*nearestNeighbours)[0].Distance {
			break
		}

		for _, e := range graph.Elements[current.Entry].Indices {
			d := distance(query.Vector, graph.Elements[e].Vector)

			if val, ok := visited[e]; ok {
				if val[d] {
					continue
				}
			}

			if _, ok := visited[e]; ok {
				visited[e][d] = true
			} else {
				visited[e] = map[float64]bool{d: true}
			}

			if d < (*nearestNeighbours)[0].Distance || nearestNeighbours.Len() < ef {
				heap.Push(candidateHeap, Candidate{Distance: d, Entry: e})
				heap.Push(nearestNeighbours, Candidate{Distance: d, Entry: e})
				if nearestNeighbours.Len() > ef {
					_ = heap.Pop(nearestNeighbours)
				}
			}
		}
	}

	return *nearestNeighbours
}

func distance(a, b []float64) float64 {
	dotProduct := 0.0
	magnitudeA := 0.0
	magnitudeB := 0.0

	for i := 0; i < len(a); i++ {
		dotProduct += a[i] * b[i]
		magnitudeA += a[i] * a[i]
		magnitudeB += b[i] * b[i]
	}

	magnitudeA = math.Sqrt(magnitudeA)
	magnitudeB = math.Sqrt(magnitudeB)

	return 1.0 - (dotProduct / (magnitudeA * magnitudeB))
}

func (hnsw *HNSW) Create(dataset []VectorNode) {
	for _, v := range dataset {
		hnsw.insert(v)
	}
}

func (hnsw *HNSW) Search(query VectorNode, ef int) []Match {
	if len(hnsw.Index[0].Elements) == 0 {
		return []Match{}
	}

	bestNode := 0
	for _, graph := range hnsw.Index {
		nn := hnsw.searchLayer(graph, bestNode, query, 1)[0]
		bestNode = nn.Entry
		if graph.Elements[bestNode].Entry > 0 {
			bestNode = graph.Elements[bestNode].Entry
		} else {
			neighbours := hnsw.searchLayer(graph, bestNode, query, ef)
			result := []Match{}
			for _, neighbour := range neighbours {
				result = append(result,
					Match{
						Offsets: []Position{{DocumentID: float64(hnsw.Index[len(hnsw.Index)-1].Elements[neighbour.Entry].ID)}},
						Score:   neighbour.Distance,
					},
				)
			}
			return result
		}
	}
	return []Match{}
}

func (hnsw *HNSW) getInsertLayer() int {
	l := -math.Log(rand.Float64()) * hnsw.mL
	return int(math.Min(l, float64(hnsw.L-1)))
}

func (hnsw *HNSW) insert(vec VectorNode) {
	if len(hnsw.Index[0].Elements) == 0 {
		i := -1
		for n := len(hnsw.Index) - 1; n >= 0; n-- {
			vec.Entry = i
			hnsw.Index[n] = Graph{Elements: []VectorNode{{ID: vec.ID, Vector: vec.Vector, Entry: i}}}
			i = 0
		}
		return
	}

	l := hnsw.getInsertLayer()
	startingNode := 0
	for i := range hnsw.Index {
		if i < l {
			startingNode = hnsw.searchLayer(hnsw.Index[i], startingNode, vec, 1)[0].Entry
		} else {
			entry := -1
			if i < hnsw.L-1 {
				entry = len(hnsw.Index[i+1].Elements)
			}
			node := VectorNode{Vector: vec.Vector, Indices: []int{}, Entry: entry, ID: vec.ID}

			nearestNeighbours := hnsw.searchLayer(hnsw.Index[i], startingNode, vec, hnsw.EFC)

			m := int(math.Min(float64(hnsw.M), float64(len(nearestNeighbours))))
			if len(nearestNeighbours) > hnsw.M {
				nearestNeighbours = nearestNeighbours[len(nearestNeighbours)-m-1:]
			}
			for _, neighbour := range nearestNeighbours {
				//add every NN to the new node
				node.Indices = append(node.Indices, neighbour.Entry)
				//add the new node to every NN
				hnsw.Index[i].Elements[neighbour.Entry].Indices = append(hnsw.Index[i].Elements[neighbour.Entry].Indices,
					len(hnsw.Index[i].Elements),
				)
			}
			hnsw.Index[i].Elements = append(hnsw.Index[i].Elements, node)
		}
		startingNode = hnsw.Index[i].Elements[startingNode].Entry
	}
}

func (h *HNSW) Encode() []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)

	err := enc.Encode(h)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (h *HNSW) Decode(b []byte) HNSW {
	var q HNSW
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&q)
	if err != nil {
		panic(err)
	}

	return q
}
