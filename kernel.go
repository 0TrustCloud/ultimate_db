package ultimate_db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Internal Registry Reservations
const (
	IndexPageID         PageID = 10
	MetadataPageID      PageID = 11
	PostingsChunkSize          = 256
	DefaultVirtualNodes        = 64
)

// -----------------------------------------------------------------------------
// Core Interfaces (Breaking the Circular Dependency Loops)
// -----------------------------------------------------------------------------

type NetworkTransport interface {
	BroadcastQuery(ctx context.Context, queryPayload []byte) ([][]byte, error)
	GetLocalNodeID() string
}

type AuditInterceptor interface {
	VerifyAccess(subject []byte, action, resource string) bool
	LogAudit(actor, action, msg string)
}

// -----------------------------------------------------------------------------
// Core Tokenizer Subsystem
// -----------------------------------------------------------------------------

type InternalAnalyzer struct {
	stopWords map[string]bool
}

func NewInternalAnalyzer() *InternalAnalyzer {
	return &InternalAnalyzer{
		stopWords: map[string]bool{
			"the": true, "is": true, "at": true, "which": true, "on": true,
			"and": true, "a": true, "an": true, "in": true, "of": true,
			"to": true, "for": true, "with": true, "by": true, "as": true,
		},
	}
}

func (a *InternalAnalyzer) Tokenize(text string) []string {
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	rawTokens := strings.FieldsFunc(text, f)
	tokens := make([]string, 0, len(rawTokens))
	
	for _, t := range rawTokens {
		clean := strings.ToLower(t)
		if !a.stopWords[clean] && len(clean) > 1 {
			tokens = append(tokens, clean)
		}
	}
	return tokens
}

// -----------------------------------------------------------------------------
// Data Transmission Structures
// -----------------------------------------------------------------------------

type Posting struct {
	DocID string  `json:"doc_id"`
	TF    float64 `json:"tf"`
}

type PostingsChunk struct {
	ChunkID  uint32    `json:"chunk_id"`
	Postings []Posting `json:"postings"`
}

type EngineState struct {
	TotalDocs int     `json:"total_docs"`
	AvgDocLen float64 `json:"avg_doc_len"`
}

type SearchResult struct {
	DocID string  `json:"doc_id"`
	Score float64 `json:"score"`
}

type ClusterQuery struct {
	QueryID   string `json:"query_id"`
	QueryText string `json:"query_text"`
	Limit     int    `json:"limit"`
}

type RoutingEntry struct {
	ID      string
	Address string
	Healthy bool
}

type IntegratedEngine struct {
	DB          *DB
	Transport   NetworkTransport
	Interceptor AuditInterceptor
	Analyzer    *InternalAnalyzer
	Scorer      *BM25Scorer

	vNodes       int
	ring         []uint32
	nodeMap      map[uint32]string
	routingTable map[string]*RoutingEntry

	TotalDocs int
	AvgDocLen float64
	mu        sync.RWMutex
}

func NewIntegratedEngine(db *DB, transport NetworkTransport, interceptor AuditInterceptor) (*IntegratedEngine, error) {
	ie := &IntegratedEngine{
		DB:           db,
		Transport:    transport,
		Interceptor:  interceptor,
		Analyzer:     NewInternalAnalyzer(),
		Scorer:       NewBM25Scorer(),
		vNodes:       DefaultVirtualNodes,
		nodeMap:      make(map[uint32]string),
		routingTable: make(map[string]*RoutingEntry),
	}

	txn := db.BeginTxn()
	stateBytes, err := db.Read(MetadataPageID, txn, []byte("bm25_state"))
	db.CommitTxn(txn)

	if err == nil && len(stateBytes) > 0 {
		var state EngineState
		if err := json.Unmarshal(stateBytes, &state); err == nil {
			ie.TotalDocs = state.TotalDocs
			ie.AvgDocLen = state.AvgDocLen
		}
	}

	return ie, nil
}

func (ie *IntegratedEngine) AddClusterNode(nodeID, address string) {
	ie.mu.Lock()
	defer ie.mu.Unlock()

	ie.routingTable[nodeID] = &RoutingEntry{
		ID:      nodeID,
		Address: address,
		Healthy: true,
	}

	hasher := sha256.New()
	for i := 0; i < ie.vNodes; i++ {
		hasher.Reset()
		hasher.Write([]byte(fmt.Sprintf("%s#%d", nodeID, i)))
		hk := binary.BigEndian.Uint32(hasher.Sum(nil)[0:4])
		ie.ring = append(ie.ring, hk)
		ie.nodeMap[hk] = nodeID
	}
	sort.Slice(ie.ring, func(i, j int) bool { return ie.ring[i] < ie.ring[j] })
}

// InsertDocument runs a single-pass transaction, writing data blocks and updating index chunks simultaneously
func (ie *IntegratedEngine) InsertDocument(pageID PageID, docID string, text string) error {
	tokens := ie.Analyzer.Tokenize(text)
	termCounts := make(map[string]int)
	for _, token := range tokens {
		termCounts[token]++
	}

	txn := ie.DB.BeginTxn()
	defer ie.DB.CommitTxn(txn)

	docKey := []byte(fmt.Sprintf("doc:%s", docID))
	if err := ie.DB.Write(pageID, txn, docKey, []byte(text), 0); err != nil {
		return err
	}

	for term, count := range termCounts {
		metaKey := []byte(fmt.Sprintf("term_meta:%s", term))
		var chunkCount uint32 = 0
		
		metaBytes, err := ie.DB.Read(IndexPageID, txn, metaKey)
		if err == nil && len(metaBytes) > 0 {
			chunkCount = binary.BigEndian.Uint32(metaBytes)
		}

		if chunkCount == 0 {
			chunkCount = 1
			var buf [4]byte
			binary.BigEndian.PutUint32(buf[:], chunkCount)
			_ = ie.DB.Write(IndexPageID, txn, metaKey, buf[:], 0)
		}

		targetChunkKey := []byte(fmt.Sprintf("term:%s:chunk:%d", term, chunkCount-1))
		var currentChunk PostingsChunk
		
		chunkBytes, err := ie.DB.Read(IndexPageID, txn, targetChunkKey)
		if err == nil && len(chunkBytes) > 0 {
			_ = json.Unmarshal(chunkBytes, &currentChunk)
		} else {
			currentChunk = PostingsChunk{
				ChunkID:  chunkCount - 1,
				Postings: make([]Posting, 0, PostingsChunkSize),
			}
		}

		newPosting := Posting{DocID: docID, TF: float64(count)}

		if len(currentChunk.Postings) >= PostingsChunkSize {
			chunkCount++
			var buf [4]byte
			binary.BigEndian.PutUint32(buf[:], chunkCount)
			_ = ie.DB.Write(IndexPageID, txn, metaKey, buf[:], 0)

			targetChunkKey = []byte(fmt.Sprintf("term:%s:chunk:%d", term, chunkCount-1))
			currentChunk = PostingsChunk{
				ChunkID:  chunkCount - 1,
				Postings: []Posting{newPosting},
			}
		} else {
			currentChunk.Postings = append(currentChunk.Postings, newPosting)
		}

		updatedData, _ := json.Marshal(currentChunk)
		if err := ie.DB.Write(IndexPageID, txn, targetChunkKey, updatedData, 0); err != nil {
			return err
		}
	}

	ie.mu.Lock()
	prevDocs := ie.TotalDocs
	ie.TotalDocs++
	ie.AvgDocLen = ((ie.AvgDocLen * float64(prevDocs)) + float64(len(tokens))) / float64(ie.TotalDocs)
	state := EngineState{TotalDocs: ie.TotalDocs, AvgDocLen: ie.AvgDocLen}
	stateBytes, _ := json.Marshal(state)
	_ = ie.DB.Write(MetadataPageID, txn, []byte("bm25_state"), stateBytes, 0)
	ie.mu.Unlock()

	return nil
}

func (ie *IntegratedEngine) LocalSearch(queryText string, limit int) ([]SearchResult, error) {
	ie.mu.RLock()
	totalDocs := ie.TotalDocs
	avgDocLen := ie.AvgDocLen
	ie.mu.RUnlock()

	ast, err := ParseQuery(queryText)
	if err != nil {
		return nil, err
	}

	txn := ie.DB.BeginTxn()
	defer ie.DB.CommitTxn(txn)

	validDocs := ie.evaluateAST(ast, txn)
	if len(validDocs) == 0 {
		return nil, nil
	}

	tokens := ie.Analyzer.Tokenize(queryText)
	docScores := make(map[string]float64)

	for _, token := range tokens {
		startRange := []byte(fmt.Sprintf("term:%s:chunk:0", token))
		endRange := []byte(fmt.Sprintf("term:%s:chunk:\xff", token))

		cursor := ie.DB.NewRangeCursor(IndexPageID, txn, startRange, endRange)
		var postingsCount int
		var matches []Posting

		for {
			_, valBytes, err := cursor.Next()
			if err != nil || valBytes == nil {
				break
			}
			var chunk PostingsChunk
			if json.Unmarshal(valBytes, &chunk) == nil {
				postingsCount += len(chunk.Postings)
				matches = append(matches, chunk.Postings...)
			}
		}

		if postingsCount == 0 {
			continue
		}

		for _, p := range matches {
			if !validDocs[p.DocID] {
				continue
			}
			score := ie.Scorer.Score(p.TF, avgDocLen, avgDocLen, totalDocs, postingsCount)
			docScores[p.DocID] += score
		}
	}

	var results []SearchResult
	for id, score := range docScores {
		results = append(results, SearchResult{DocID: id, Score: score})
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

func (ie *IntegratedEngine) evaluateAST(q Query, txn uint64) map[string]bool {
	res := make(map[string]bool)
	switch v := q.(type) {
	case *TermQuery:
		startRange := []byte(fmt.Sprintf("term:%s:chunk:0", v.Term))
		endRange := []byte(fmt.Sprintf("term:%s:chunk:\xff", v.Term))
		cursor := ie.DB.NewRangeCursor(IndexPageID, txn, startRange, endRange)
		for {
			_, valBytes, err := cursor.Next()
			if err != nil || valBytes == nil {
				break
			}
			var chunk PostingsChunk
			if json.Unmarshal(valBytes, &chunk) == nil {
				for _, p := range chunk.Postings {
					res[p.DocID] = true
				}
			}
		}
	case *AndQuery:
		left := ie.evaluateAST(v.Left, txn)
		right := ie.evaluateAST(v.Right, txn)
		for k := range left {
			if right[k] { res[k] = true }
		}
	case *OrQuery:
		left := ie.evaluateAST(v.Left, txn)
		right := ie.evaluateAST(v.Right, txn)
		for k := range left { res[k] = true }
		for k := range right { res[k] = true }
	}
	return res
}

func (ie *IntegratedEngine) ScatterGather(ctx context.Context, subjectID []byte, queryText string, limit int) ([]SearchResult, error) {
	if ie.Interceptor != nil {
		if !ie.Interceptor.VerifyAccess(subjectID, "SEARCH", "audit_logs") {
			ie.Interceptor.LogAudit(string(subjectID), "SEARCH_DENIED", "ABAC validation failure on distributed search request")
			return nil, fmt.Errorf("security policy validation failure: search access denied")
		}
	}

	localHits, err := ie.LocalSearch(queryText, limit)
	if err != nil {
		return nil, err
	}

	if ie.Transport == nil {
		return localHits, nil
	}

	payload, _ := json.Marshal(ClusterQuery{
		QueryID:   fmt.Sprintf("q_%d", time.Now().UnixNano()),
		QueryText: queryText,
		Limit:     limit,
	})

	remoteResponses, err := ie.Transport.BroadcastQuery(ctx, payload)
	var globalHits []SearchResult
	globalHits = append(globalHits, localHits...)

	if err == nil {
		for _, respBytes := range remoteResponses {
			var peerHits []SearchResult
			if json.Unmarshal(respBytes, &peerHits) == nil {
				globalHits = append(globalHits, peerHits...)
			}
		}
	}

	sort.Slice(globalHits, func(i, j int) bool { return globalHits[i].Score > globalHits[j].Score })
	if limit > 0 && len(globalHits) > limit {
		globalHits = globalHits[:limit]
	}

	return globalHits, nil
}
