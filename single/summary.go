package single

import (
	"fmt"
	"github.com/alexeyco/simpletable"
	"io"
)

func PrintSummary(filename string, out io.Writer) error {

	i, err := OpenInvertedIndex(filename, DecompressUint64)
	if err != nil {
		return err
	}

	r := i.(*InvertedIndex[uint64])

	// Prepare table rows
	s, _ := r.file.Stat()
	minTerm, _ := r.fst.GetMinKey()
	maxTerm, _ := r.fst.GetMaxKey()

	mTerm, _ := r.fst.GetMinKey()
	bitmapOffset, _, _ := r.fst.Get(mTerm)
	bitmapsLen := r.fstOffset - int64(bitmapOffset)

	data := [][]interface{}{
		{"File total size", fmt.Sprintf("%d", s.Size())},
		{"FST size", fmt.Sprintf("%d", s.Size()-r.fstOffset-24)},
		{"Terms count", fmt.Sprintf("%d", r.fst.Len())},
		{"Terms min,max", fmt.Sprintf("%s, %s", string(minTerm), string(maxTerm))},
		{"Bitmaps size", fmt.Sprintf("%d", bitmapsLen)},
		{"Values size", fmt.Sprintf("%d", r.indexOffset)},
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
