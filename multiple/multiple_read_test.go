package multiple

import (
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"inverted-index/single"
	"os"
	"path/filepath"
	"testing"
)

func TestMultipleRead(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)

	// 1. Create 2 files
	filename1 := filepath.Join(dirPath, "index1")
	w1, err := single.NewInvertedIndexUnit[uint32](filename1, 2, single.CompressUint32, single.DecompressUint32)
	require.NoError(t, err)

	require.NoError(t, w1.Put("term1", []uint32{1, 3, 5, 7, 9}))
	require.NoError(t, w1.Put("term2", []uint32{10, 30, 50, 70, 90}))
	require.NoError(t, w1.Put("term3", []uint32{15}))
	require.NoError(t, w1.Close())

	filename2 := filepath.Join(dirPath, "index2")
	w2, err := single.NewInvertedIndexUnit[uint32](filename2, 2, single.CompressUint32, single.DecompressUint32)
	require.NoError(t, err)

	require.NoError(t, w2.Put("term0", []uint32{500, 600}))
	require.NoError(t, w2.Put("term1", []uint32{0, 2, 4, 6, 8}))
	require.NoError(t, w2.Put("term2", []uint32{20, 40, 60, 80}))
	require.NoError(t, w2.Close())

	// 2. Read terms from 2 files
	rt, err := NewMultipleTermsReader([]string{filename1, filename2})
	require.NoError(t, err)

	actualT := lezhnev74.ToSlice(rt)
	expectedT := []string{"term0", "term1", "term2", "term3"}
	require.EqualValues(t, expectedT, actualT)

	require.NoError(t, rt.Close())
	// 3. Read values from 2 files
	rv, err := NewMultipleValuesReader[uint32](
		[]string{filename1, filename2},
		single.DecompressUint32,
		[]string{"term1", "term2"},
		0,
		999999,
	)
	require.NoError(t, err)

	actual := lezhnev74.ToSlice(rv)
	require.NoError(t, rv.Close())

	expected := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90}

	require.EqualValues(t, expected, actual)
}
