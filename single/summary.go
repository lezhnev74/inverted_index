package single

import (
	"fmt"
	"github.com/alexeyco/simpletable"
	"io"
)

func PrintSummary(filename string, out io.Writer) error {

	i, err := OpenInvertedIndex(filename, DecompressUint32)
	if err != nil {
		return err
	}

	r := i.(*InvertedIndex[uint32])

	// Prepare table rows
	s, _ := r.file.Stat()
	minTerm, _ := r.fst.GetMinKey()
	maxTerm, _ := r.fst.GetMaxKey()

	mTerm, _ := r.fst.GetMinKey()
	bitmapOffset, _, _ := r.fst.Get(mTerm)
	bitmapsLen := r.fstOffset - int64(bitmapOffset)
	idx, _ := r.readValuesIndex()

	data := [][]interface{}{
		{"File total size", fmt.Sprintf("%d", s.Size())},
		{"FST size", fmt.Sprintf("%d", s.Size()-r.fstOffset-24)},
		{"Terms count", fmt.Sprintf("%d", r.fst.Len())},
		{"Terms min,max", fmt.Sprintf("%s, %s", string(minTerm), string(maxTerm))},
		{"Bitmaps size", fmt.Sprintf("%d", bitmapsLen)},
		{"Values segments count/size", fmt.Sprintf("%d/%d", len(idx), r.segmentSize)},
		{"Values index size", fmt.Sprintf("%d", int64(bitmapOffset)-r.indexOffset)},
		{"Values size", fmt.Sprintf("%d", r.indexOffset)},
		{"Values count (approx.)", fmt.Sprintf("%d-%d", max(len(idx)-1, 0)*int(r.segmentSize), len(idx)*int(r.segmentSize))},
		{"Values min,max", fmt.Sprintf("%v, %v", r.minVal, r.maxVal)},
	}

	table := simpletable.New()
	for _, row := range data {
		r := []*simpletable.Cell{
			{Text: row[0].(string)},
			{Align: simpletable.AlignRight, Text: row[1].(string)},
		}

		table.Body.Cells = append(table.Body.Cells, r)
	}
	fmt.Fprintln(out, "\nIndex File Summary")
	fmt.Fprintln(out, table.String())

	return nil
}
