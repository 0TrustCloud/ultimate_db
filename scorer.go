package ultimate_db

import (
	"math"
)

type BM25Scorer struct {
	k1 float64
	b  float64
}

func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{
		k1: 1.2,
		b:  0.75,
	}
}

func (s *BM25Scorer) Score(tf, docLen, avgDocLen float64, totalDocs, docFreq int) float64 {
	if tf <= 0 || totalDocs <= 0 {
		return 0
	}

	if docFreq <= 0 {
		docFreq = 1
	}

	if avgDocLen <= 0 {
		avgDocLen = 1
	}

	if docLen <= 0 {
		docLen = 1
	}

	numerator := float64(totalDocs-docFreq) + 0.5
	denominator := float64(docFreq) + 0.5
	idf := math.Log(1 + (numerator / denominator))

	lengthNorm := 1 - s.b + s.b*(docLen/avgDocLen)
	tfNorm := (tf * (s.k1 + 1)) / (tf + s.k1*lengthNorm)

	return idf * tfNorm
}
