package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"strings"

	"github.com/farouqzaib/fast-search/internal/analyzer"
)

type InvertedIndex struct {
	PostingsList map[string]SkipList
}

func NewInvertedIndex() *InvertedIndex {
	postingsList := map[string]SkipList{}
	return &InvertedIndex{
		PostingsList: postingsList,
	}
}

func (i *InvertedIndex) ConcurrentIndex(docID int, tokens []string) {
	tokenOffsets := map[string][]int{}

	for j, token := range tokens {
		_, ok := tokenOffsets[token]
		if ok {
			tokenOffsets[token] = append(tokenOffsets[token], j)
		} else {
			tokenOffsets[token] = []int{j}
		}
	}

	tokensCh := make(chan map[string][]int, len(tokenOffsets))
	resultCh := make(chan map[string]SkipList, len(tokenOffsets))

	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		go func(tokenCh chan map[string][]int) {
			for tokenOffset := range tokenCh {
				sk := *NewSkipList()
				token := ""

				for tok, offsets := range tokenOffset {
					token = tok
					for _, t := range offsets {
						sk.Insert(Position{DocumentID: float64(docID), Offset: float64(t)})
					}
				}

				resultCh <- map[string]SkipList{token: sk}
			}
		}(tokensCh)
	}

	for token, offsets := range tokenOffsets {
		tokensCh <- map[string][]int{token: offsets}
	}

	for k := 0; k < len(tokenOffsets); k++ {
		result := <-resultCh
		for k, v := range result {
			i.PostingsList[k] = v
		}
	}
}

func (i *InvertedIndex) BulkIndex(docIDs []int, documents []string) {
	// slog.Info("index: indexing documents", slog.Int("docID", docID))
	// tokens := analyzer.Analyze(document)

	// i.ConcurrentIndex(docID, tokens)

	// for j, word := range tokens {
	// 	_, ok := i.PostingsList[word]

	// 	if !ok {
	// 		i.PostingsList[word] = *NewSkipList()
	// 	}

	// 	sk := i.PostingsList[word]
	// 	sk.Insert(Position{DocumentID: float64(docID), Offset: float64(j)})
	// 	i.PostingsList[word] = sk
	// }
}

func (i *InvertedIndex) Index(docID int, document string) {
	slog.Info("index: indexing documents", slog.Int("docID", docID))
	tokens := analyzer.Analyze(document)

	i.ConcurrentIndex(docID, tokens)

	// for j, word := range tokens {
	// 	_, ok := i.PostingsList[word]

	// 	if !ok {
	// 		i.PostingsList[word] = *NewSkipList()
	// 	}

	// 	sk := i.PostingsList[word]
	// 	sk.Insert(Position{DocumentID: float64(docID), Offset: float64(j)})
	// 	i.PostingsList[word] = sk
	// }
}

func (i *InvertedIndex) First(token string) (Position, error) {
	_, ok := i.PostingsList[token]

	if ok {
		sk := i.PostingsList[token]
		return sk.Head.Tower[0].Key, nil
	}
	return Position{DocumentID: EOF, Offset: EOF}, errors.New("no list exists for token")
}

func (i *InvertedIndex) Last(token string) (Position, error) {
	_, ok := i.PostingsList[token]

	if ok {
		sk := i.PostingsList[token]
		return sk.Last(), nil
	}
	return Position{DocumentID: EOF, Offset: EOF}, errors.New("no list exists for token")
}

func (i *InvertedIndex) Next(token string, offset Position) (Position, error) {
	if offset.Offset == BOF {
		return i.First(token)
	}

	if offset.Offset == EOF {
		return Position{DocumentID: EOF, Offset: EOF}, nil
	}

	_, ok := i.PostingsList[token]

	if ok {
		sk := i.PostingsList[token]

		key, _ := sk.FindGreaterThan(offset)
		return key, nil
	}

	return Position{DocumentID: EOF, Offset: EOF}, errors.New("no list exists for token")
}

func (i *InvertedIndex) Previous(token string, offset Position) (Position, error) {
	if offset.Offset == EOF {
		return i.Last(token)
	}

	if offset.Offset == BOF {
		return Position{DocumentID: BOF, Offset: BOF}, nil
	}

	_, ok := i.PostingsList[token]

	if ok {
		sk := i.PostingsList[token]
		key, _ := sk.FindLessThan(offset)
		return key, nil
	}

	return Position{DocumentID: BOF, Offset: BOF}, errors.New("no list exists for token")
}

func (i *InvertedIndex) NextPhrase(query string, offset Position) []Position {
	v := offset

	terms := strings.Fields(query)
	for _, char := range terms {
		word := string(char)
		v, _ = i.Next(word, v)
	}

	if v.Offset == EOF {
		return []Position{{DocumentID: EOF, Offset: EOF}, {DocumentID: EOF, Offset: EOF}}
	}

	u := v

	for j := len(terms) - 2; j >= 0; j-- {
		word := terms[j]
		u, _ = i.Previous(word, u)
	}

	if (v.DocumentID == u.DocumentID) && (v.GetOffset()-u.GetOffset() == len(strings.Fields(query))-1) {
		return []Position{u, v}
	}

	return i.NextPhrase(query, u)
}

func (i *InvertedIndex) FindAllPhrases(query string, offset Position) [][]Position {
	u := Position{DocumentID: BOF, Offset: BOF}

	positions := [][]Position{}

	for u.DocumentID != EOF {
		offsets := i.NextPhrase(query, u)
		u = offsets[0]

		if u.DocumentID != EOF && u.Offset != EOF {
			positions = append(positions, offsets)
		}
	}

	return positions
}

func (i *InvertedIndex) NextCover(tokens []string, offset Position) []Position {
	v := offset

	for j, word := range tokens {
		localMax, _ := i.Next(word, offset)

		//break if localMax is ever EOF
		if localMax.DocumentID == EOF {
			v = localMax
			break
		}

		if j == 0 {
			v = localMax
			continue
		}

		if localMax.DocumentID > v.DocumentID || (localMax.DocumentID == v.DocumentID && localMax.Offset > v.Offset) {
			v = localMax
		}
	}

	if v.DocumentID == EOF {
		return []Position{{DocumentID: EOF, Offset: EOF}, {DocumentID: EOF, Offset: EOF}}
	}

	u := Position{DocumentID: BOF, Offset: BOF}

	for j, word := range tokens {
		localMin, _ := i.Previous(word, Position{DocumentID: v.DocumentID, Offset: v.Offset + 1})

		if j == 0 {
			u = localMin
			continue
		}

		if localMin.DocumentID < u.DocumentID || (localMin.Offset == u.Offset && localMin.Offset < u.Offset) {
			u = localMin
		}
	}

	if u.DocumentID == v.DocumentID {
		return []Position{u, v}
	}

	return i.NextCover(tokens, u)
}

type Match struct {
	Offsets []Position
	Score   float64
}

func (i *InvertedIndex) RankProximity(query string, k int) []Match {
	slog.Info("index: proximity ranking")
	tokens := analyzer.Analyze(query)
	slog.Info("index: search tokens", slog.String("tokens", fmt.Sprintf("%v", tokens)))
	if len(tokens) == 0 {
		return []Match{}
	}

	offsets := i.NextCover(tokens, Position{DocumentID: BOF, Offset: BOF})
	u, v := offsets[0], offsets[1]
	candidate := []Position{u, v}
	score := 0.0
	results := []Match{}

	for u.DocumentID < EOF {
		if candidate[0].DocumentID < u.DocumentID {
			results = append(results, Match{Offsets: candidate, Score: score})
			candidate = []Position{u, v}
			score = 0
		}

		score = score + 1/(v.Offset-u.Offset+1)

		offsets = i.NextCover(tokens, u)
		u, v = offsets[0], offsets[1]
	}

	if candidate[0].DocumentID < EOF {
		results = append(results, Match{Offsets: candidate, Score: score})
	}

	return results[:int(math.Min(float64(k), float64(len(results))))]
}

func (i *InvertedIndex) Encode() []byte {
	b := new(bytes.Buffer)
	// termList := []string{}
	for k, v := range i.PostingsList {

		// termList = append(termList, k)
		//add len of title to buffer
		binary.Write(b, binary.LittleEndian, uint32(len([]byte(k))))

		//add title to buffer
		b.Write([]byte(k))

		curr := v.Head

		type truncatedOffset struct {
			DocId    uint32
			Position uint32
		}
		//get all the keys first
		nodes := map[truncatedOffset]int{}

		head := curr
		counter := 1

		nodeBytes := new(bytes.Buffer)
		// nodeEncoder := gob.NewEncoder(nodeBytes)
		for head != nil {
			offset := truncatedOffset{DocId: uint32(head.Key.DocumentID), Position: uint32(head.Key.Offset)}
			nodes[offset] = counter
			counter++

			err := binary.Write(nodeBytes, binary.LittleEndian, uint32(head.Key.DocumentID))

			if err != nil {
				panic(err)
			}

			_ = binary.Write(nodeBytes, binary.LittleEndian, uint32(head.Key.Offset))

			if err != nil {
				panic(err)
			}

			head = head.Tower[0]

		}

		//add len of nodes to buffer
		// b.Write([]byte(strconv.Itoa(len(nodeBytes.Bytes()))))
		binary.Write(b, binary.LittleEndian, uint32(len(nodeBytes.Bytes())))

		//add nodes to buffer
		b.Write(nodeBytes.Bytes())

		for curr != nil {
			tower := []Node{}
			for level := 0; level < MaxHeight; level++ {
				if curr.Tower[level] == nil {
					break
				}

				tower = append(tower, *curr.Tower[level])
			}

			towerKeys := []uint16{}

			towerNodeBytes := new(bytes.Buffer)
			// towerNodeEncoder := gob.NewEncoder(towerNodeBytes)
			for _, node := range tower {
				offset := truncatedOffset{DocId: uint32(node.Key.DocumentID), Position: uint32(node.Key.Offset)}
				towerKeys = append(towerKeys, uint16(nodes[offset]))

				err := binary.Write(towerNodeBytes, binary.LittleEndian, uint16(nodes[offset]))

				if err != nil {
					panic(err)
				}
			}

			if len(towerKeys) == 0 {
				nilTowerNodeBytes := new(bytes.Buffer)

				err := binary.Write(nilTowerNodeBytes, binary.LittleEndian, uint16(0))

				if err != nil {
					panic(err)
				}
				//add len of tower nodes to buffer
				binary.Write(b, binary.LittleEndian, uint32(len(nilTowerNodeBytes.Bytes())))

				//add tower nodes to buffer
				b.Write(nilTowerNodeBytes.Bytes())
			} else {
				// add len of tower nodes to buffer
				binary.Write(b, binary.LittleEndian, uint32(len(towerNodeBytes.Bytes())))

				// add tower nodes to buffer
				b.Write(towerNodeBytes.Bytes())
			}

			curr = curr.Tower[0]
		}
	}

	return b.Bytes()
}

func (i *InvertedIndex) Decode(b []byte) InvertedIndex {
	recoveredIndex := map[string]SkipList{}

	offset := 0
	round := 0
	for offset < len(b) {
		// fmt.Println("len", len(b.Bytes()))
		n := int(binary.LittleEndian.Uint32(b[offset : offset+4]))
		// fmt.Println("term index", n)

		// n, _ := strconv.Atoi(string(b.Bytes()[offset]))
		// fmt.Println(n)
		offset = offset + 4
		// fmt.Printf("actual: %s, recovered: %s\n\n", termList[round], string(b.Bytes()[offset:offset+n]))

		term := string(b[offset : offset+n])

		//get number of bytes for nodes
		offset = offset + n
		un := binary.LittleEndian.Uint32(b[offset : offset+4])
		// n, _ = strconv.Atoi(string(b.Bytes()[offset : offset+1]))
		// fmt.Println(un)

		// os.Exit(-1)
		positions := []*Node{}
		positionMap := map[int]*Node{}
		counter := 1
		offset = offset + 4
		for i := 0; i < int(un)/4; i++ {
			node := binary.LittleEndian.Uint32(b[offset : offset+4])

			if i%2 == 0 {
				positions = append(positions, &Node{Key: Position{DocumentID: float64(node)}})
			} else {
				positions[len(positions)-1].Key.Offset = float64(node)
				positionMap[counter] = positions[len(positions)-1]
				counter++
			}
			offset = offset + 4
		}

		height := 1.0
		//loop for each of the nodes found
		for i := 1; i <= int(un)/8; i++ {
			//get length of node tower keys
			kn := binary.LittleEndian.Uint32(b[offset : offset+4])
			// fmt.Println("len node keys", kn)

			towerKeys := []*Node{}
			offset = offset + 4
			for j := 0; j < int(kn)/2; j++ {
				node := binary.LittleEndian.Uint16(b[offset : offset+2])
				if node != 0 {
					towerKeys = append(towerKeys, positionMap[int(node)])
					height = math.Max(float64(len(towerKeys)), float64(height))
					positionMap[i].Tower[j] = positionMap[int(node)]
				}
				offset = offset + 2
			}
		}

		// fmt.Println("Height:", height)

		sk := SkipList{
			Head:   positionMap[1],
			Height: int(height),
		}

		recoveredIndex[term] = sk

		round++
	}

	return InvertedIndex{PostingsList: recoveredIndex}
}
