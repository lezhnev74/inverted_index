package single

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"
	"testing"
	"time"
)

func TestCompressBytes(t *testing.T) {
	nums := []uint32{}

	rand.Seed(uint64(time.Now().UnixNano()))
	for i := 0; i < 1000; i++ {
		nums = append(nums, rand.Uint32())
	}
	slices.Sort(nums)

	bs := make([]byte, 0, len(nums)*4)
	b := make([]byte, 4)
	for _, n := range nums {
		binary.BigEndian.PutUint32(b, n)
		bs = append(bs, b...)
	}

	encoded, err := compressBytes(bs)
	require.NoError(t, err)

	fmt.Printf("nums count: %d\n", len(nums))
	fmt.Printf("nums bytes: %d\n", len(bs))
	fmt.Printf("compressed bytes: %d\n", len(encoded))

	output, err := decompressBytes(encoded)
	require.NoError(t, err)

	require.EqualValues(t, bs, output)
}

func TestUint32CompressionRatioFuncs(t *testing.T) {
	size := 1_000

	input := make([]uint32, size)
	for i := 0; i < len(input); i++ {
		input[i] = rand.Uint32()
	}
	slices.Sort(input) // for better compression sort values

	encoded, err := CompressUint32(input)
	require.NoError(t, err)

	uncompressedEstimatedSize := size * 4
	actualSize := len(encoded)

	fmt.Printf("unc:\t%d\ncom:\t%d\nratio:\t%f%%\n", uncompressedEstimatedSize, actualSize, float64(actualSize)/float64(uncompressedEstimatedSize)*100)
}

func TestUint32CompressionFuncs(t *testing.T) {
	input := []uint32{1}
	encoded, err := CompressUint32(input)
	require.NoError(t, err)

	output, err := DecompressUint32(encoded)
	require.NoError(t, err)

	require.EqualValues(t, input, output)
}

func TestGobFuncs(t *testing.T) {
	input := []uint32{1}
	encoded, err := CompressGob(input)
	require.NoError(t, err)

	output, err := DecompressGob[uint32](encoded)
	require.NoError(t, err)

	require.EqualValues(t, input, output)
}
