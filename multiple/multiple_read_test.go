package multiple

import (
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"inverted-index/single"
	"os"
	"path/filepath"
	"testing"
)

func TestMultipleValuesRead(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)

	// 1. Create 2 files
	filename1 := filepath.Join(dirPath, "index1")
	w1, err := single.NewInvertedIndexUnit[uint32](filename1, 2, single.CompressUint32, single.DecompressUint32)
	require.NoError(t, err)

	require.NoError(t, w1.Put("term1", []uint32{1, 3, 5, 7, 9}))
	require.NoError(t, w1.Put("term2", []uint32{10, 30, 50, 70, 90}))
	require.NoError(t, w1.Close())

	filename2 := filepath.Join(dirPath, "index2")
	w2, err := single.NewInvertedIndexUnit[uint32](filename2, 2, single.CompressUint32, single.DecompressUint32)
	require.NoError(t, err)

	require.NoError(t, w2.Put("term1", []uint32{0, 2, 4, 6, 8}))
	require.NoError(t, w2.Put("term2", []uint32{20, 40, 60, 80}))
	require.NoError(t, w2.Close())

	// 2. Read from 2 files
	r, err := NewMultipleValuesReader[uint32](
		[]string{filename1, filename2},
		single.DecompressUint32,
		[]string{"term1", "term2"},
		0,
		999999,
	)
	require.NoError(t, err)

	// 3. Compare results
	actual := lezhnev74.ToSlice(r)
	require.NoError(t, r.Close())

	expected := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90}

	require.EqualValues(t, expected, actual)
}
