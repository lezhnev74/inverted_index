package single

import (
	"fmt"
	"io"
)

func PrintSummary(filename string, out io.Writer) error {

	i, err := OpenInvertedIndex(filename, DecompressUint32)
	if err != nil {
		return err
	}

	r := i.(*InvertedIndex[uint32])

	// Show file stats
	fmt.Fprintf(out, "File summary: \n")

	s, _ := r.file.Stat()
	fmt.Fprintf(out, "size: %d\n", s.Size())
	fmt.Fprintf(out, "FST size: %d\n", s.Size()-r.fstOffset-20) // 20 bytes for the footer
	fmt.Fprintf(out, "terms: %d\n", r.fst.Len())

	mTerm, _ := r.fst.GetMinKey()
	bitmapOffset, _, _ := r.fst.Get(mTerm)
	bitmapsLen := r.fstOffset - int64(bitmapOffset)
	fmt.Fprintf(out, "bitmaps size: %d\n", bitmapsLen)

	idx, _ := r.readValuesIndex()

	fmt.Fprintf(out, "segments count/size: %d/%d\n", len(idx), r.segmentSize)
	fmt.Fprintf(out, "values count: %d-%d\n", max(len(idx)-1, 0)*int(r.segmentSize), len(idx)*int(r.segmentSize))
	fmt.Fprintf(out, "values index size: %d\n", int64(bitmapOffset)-r.indexOffset)
	fmt.Fprintf(out, "values size: %d\n", r.indexOffset)

	return nil
}
