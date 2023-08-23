package single

import (
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestReadWrite(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 10)
	require.NoError(t, err)
	err = indexWriter.Put("term1", []int{10, 20})
	require.NoError(t, err)
	err = indexWriter.Put("term2", []int{1})
	require.NoError(t, err)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode, read terms only
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)
	termsIterator, err := indexReader.ReadTerms()
	require.NoError(t, err)
	terms := lezhnev74.ToSlice(termsIterator)
	require.EqualValues(t, []string{"term1", "term2"}, terms)

	// 2.1 Read again (is it idempotent?)
	termsIterator, err = indexReader.ReadTerms()
	require.NoError(t, err)
	terms = lezhnev74.ToSlice(termsIterator)
	require.EqualValues(t, []string{"term1", "term2"}, terms)

	// 2.2 Read values too
	valuesIterator, err := indexReader.ReadValues([]string{"term1", "term2"}, 0, math.MaxInt)
	require.NoError(t, err)
	timestamps := lezhnev74.ToSlice(valuesIterator)
	require.EqualValues(t, []int{1, 10, 20}, timestamps)
}
