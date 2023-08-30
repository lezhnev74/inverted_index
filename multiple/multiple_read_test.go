package multiple

import (
	"fmt"
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted-index/single"
	"github.com/stretchr/testify/require"

	"os"
	"path/filepath"
	"testing"
)

func TestMerging(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)

	// 1. Populate files
	type indexTermData struct {
		term   string
		values []uint32
	}
	existingIndexes := [][]indexTermData{
		{ // todo, duplicate terms in indexes
			indexTermData{"term1", []uint32{1, 6, 7}},
			indexTermData{"term2", []uint32{10}},
			indexTermData{"term3", []uint32{}},
		},
		{
			indexTermData{"term1", []uint32{15, 20}},
			indexTermData{"term4", []uint32{21, 22, 23, 24, 25}},
		},
		{
			indexTermData{"term0", []uint32{500, 1, 99}},
			indexTermData{"term5", []uint32{9, 10, 11}},
		},
	}

	files := make([]string, 0)
	for i, e := range existingIndexes {
		filePath := filepath.Join(dirPath, fmt.Sprintf("file%d", i))
		files = append(files, filePath)

		w, err := single.NewInvertedIndexUnit(filePath, 2, single.CompressUint32, single.DecompressUint32)
		require.NoError(t, err)

		for _, id := range e {
			require.NoError(t, w.Put(id.term, id.values))
		}
		require.NoError(t, w.Close())
	}

	// 2. Merge to a new index
	newFilePath := filepath.Join(dirPath, "newFile")
	err = MergeIndexes(files, newFilePath, 2, single.CompressUint32, single.DecompressUint32)
	require.NoError(t, err)

	// 3. Check the new index
	r, err := single.OpenInvertedIndex(newFilePath, single.DecompressUint32)
	require.NoError(t, err)

	expectedTerms := []string{"term0", "term1", "term2", "term3", "term4", "term5"}

	tit, err := r.ReadTerms()
	require.NoError(t, err)
	actualTerms := lezhnev74.ToSlice(tit)

	require.EqualValues(t, expectedTerms, actualTerms)
}

func TestSelectingForMerge(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)

	// since the func uses only file sizes, we can create N files manually
	files := make([]string, 0)
	for i := 0; i < 15; i++ {
		filePath := filepath.Join(dirPath, fmt.Sprintf("file%d", i))
		files = append(files, filePath)
		err = os.WriteFile(filePath, make([]byte, i), 0666)
		require.NoError(t, err)
	}

	selectedFiles, err := SelectFilesForMerging(files, 2, 4)
	require.NoError(t, err)
	expected := files[:4]
	require.EqualValues(t, expected, selectedFiles)
}

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
