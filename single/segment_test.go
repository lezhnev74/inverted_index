package single

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"
	"testing"
	"time"
)

func TestUint32CompressionRatioFuncs(t *testing.T) {
	size := 1_000

	input := make([]uint32, size)
	for i := 0; i < len(input); i++ {
		input[i] = uint32(time.Now().Add(time.Hour * time.Duration(rand.Uint32()%1000)).Unix())
	}
	slices.Sort(input) // for better compression sort values

	encoded, err := compressUint32(input)
	require.NoError(t, err)

	uncompressedEstimatedSize := size * 4
	actualSize := len(encoded)

	fmt.Printf("unc:\t%d\ncom:\t%d\nratio:\t%f%%\n", uncompressedEstimatedSize, actualSize, float64(actualSize)/float64(uncompressedEstimatedSize)*100)
}

func TestUint32CompressionFuncs(t *testing.T) {
	input := []uint32{1}
	encoded, err := compressUint32(input)
	require.NoError(t, err)

	output, err := decompressUint32(encoded)
	require.NoError(t, err)

	require.EqualValues(t, input, output)
}

func TestGobFuncs(t *testing.T) {
	input := []uint32{1}
	encoded, err := compressGob(input)
	require.NoError(t, err)

	output, err := decompressGob[uint32](encoded)
	require.NoError(t, err)

	require.EqualValues(t, input, output)
}
