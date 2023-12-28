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

	fileSize := r.mmapFile.Len()
	_, fstOffset, fstLen, _, _, _, err := r.readFooter()
	if err != nil {
		return err
	}

	// Prepare table rows

	minTerm, _ := r.fst.GetMinKey()
	maxTerm, _ := r.fst.GetMaxKey()

	data := [][]interface{}{
		{"File total size", fmt.Sprintf("%d", fileSize)},
		{"FST size", fmt.Sprintf("%d", fstLen)},
		{"Terms count", fmt.Sprintf("%d", r.fst.Len())},
		{"Terms min,max", fmt.Sprintf("%s, %s", string(minTerm), string(maxTerm))},
		{"Values size", fmt.Sprintf("%d", fstOffset)},
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
