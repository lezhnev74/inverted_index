package single

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/lezhnev74/go-iterators"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
	"io"
	"os"
	"sync"
)

/**

		File Layout:

                            ┌──────┬──────────┬─────────────────┐
                            │FSTLen│SegmentLen│ValuesIndexOffset│
                            └──────┴──────────┴─────────────────┘
                             \          _______________________/
                              \        /
            ┌──────┬───────┬───┬──────┐
            │Values│Bitmaps│FST│Footer│
            └──────┴───────┴───┴──────┘
          /         \__________________________
         |                                     \
         ┌────────┬─────┬────────┬────────┬─────┐
         │Segment1│ ... │SegmentN│IndexLen│Index│
         └────────┴─────┴────────┴────────┴─────┘


*/

var (
	ErrDuplicateTerm = fmt.Errorf("duplicate term inserted")
	ErrEmptyIndex    = fmt.Errorf("no terms inserted")
)

// InvertedIndex is a single index piece consists of terms FST + sorted values for each term
// resembles a sorted map[string][]int
// NOT CONCURRENT
type InvertedIndex[V cmp.Ordered] struct {
	file       *os.File
	closeFile  sync.Once
	filePos    int64
	buf4, buf8 []byte // len buf
	/*
		segmentSize specifies the number of values in a segment.
		The smaller the segment the less read we hopefully do,
		However too small size increases the index and fseeks.
	*/
	segmentSize uint32

	// Generic Values --------------------------------------------------
	/*
		serializeSegment is user-provided function to compress values
		since Value is generic.
	*/
	serializeSegment   func(items []V) ([]byte, error)
	unserializeSegment func(data []byte) (items []V, err error)
	cmp                func(a, b V) int // -1,0,1 to impose order on values

	// Writer-mode -----------------------------------------------------
	// Accumulates data and writes our on Close()
	fstBuilder   *vellum.Builder
	bytesWritten uint32
	// lastTerm is used to prevent term duplicates, ingested terms must be sorted and unique
	lastTerm string
	/*
		termsValues are used to keep ingested data in memory, upon Close() actual writing is happening.
		Writing makes a unique list of all values and generates bitmaps for each term in the index.
		That allows to greatly reduce the index file size.
	*/
	termsValues [][]V // temporary values for each term

	// Reader-mode -----------------------------------------------------
	// Reads immutable data in the file
	// fst allows to compress terms in the index file
	fst                    *vellum.FST
	fstBuf                 *bytes.Buffer
	fstOffset, indexOffset int64
}

type InvertedIndexWriter[V constraints.Ordered] interface {
	io.Closer // flush FST
	// Put must be called so terms are sorted, values must also be sorted beforehand
	Put(term string, values []V) error
}

type InvertedIndexReader[V constraints.Ordered] interface {
	ReadTerms() (lezhnev74.Iterator[string], error)
	// ReadValues returns sorted iterator
	ReadValues(terms []string, min V, max V) (lezhnev74.Iterator[V], error)
	io.Closer
}

func (i *InvertedIndex[V]) Close() error {
	defer i.closeFile.Do(func() { i.file.Close() })

	if i.fstBuilder != nil {
		if len(i.termsValues) == 0 {
			return ErrEmptyIndex
		}
		return i.write()
	}

	return nil
}

// Put remembers all terms and its values, actual writing is delayed until Close()
func (i *InvertedIndex[V]) Put(term string, values []V) error {

	if i.lastTerm == term {
		return ErrDuplicateTerm
	}
	i.lastTerm = term

	err := i.fstBuilder.Insert([]byte(term), uint64(len(i.termsValues)))
	if err != nil {
		return err
	}

	i.termsValues = append(i.termsValues, values)

	return nil
}

func (i *InvertedIndex[V]) ReadValues(terms []string, minVal V, maxVal V) (lezhnev74.Iterator[V], error) {

	if len(terms) == 0 {
		return lezhnev74.NewSliceIterator([]V{}), nil
	}

	// Note:
	// Read all terms bitmaps and join into one.
	// Use it to select segments for reading.

	bs, err := i.readBitmaps(terms)
	if err != nil {
		return nil, fmt.Errorf("bitmaps: %w", err)
	}
	if len(bs) == 0 {
		return lezhnev74.NewSliceIterator([]V{}), nil
	}

	b := bs[0]
	for j := 1; j < len(bs); j++ {
		b.Or(bs[j])
	}

	segmentsIndex, err := i.readValuesIndex()
	if err != nil {
		return nil, fmt.Errorf("read values index: %w", err)
	}

	segmentsIndex = i.selectSegments(segmentsIndex, b, minVal, maxVal)

	valuesFetchFunc, err := i.makeSegmentsFetchFunc(segmentsIndex, b, minVal, maxVal)
	if err != nil {
		return nil, fmt.Errorf("failed reading term values: %w", err)
	}

	closeIterator := func() error {
		i.closeFile.Do(func() { i.file.Close() })
		return nil
	}
	it := lezhnev74.NewDynamicSliceIterator(valuesFetchFunc, closeIterator)

	return it, nil
}

func (i *InvertedIndex[V]) ReadTerms() (lezhnev74.Iterator[string], error) {
	it, err := i.fst.Iterator(nil, nil)
	if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
		return nil, err
	} else if errors.Is(err, vellum.ErrIteratorDone) {
		return lezhnev74.NewSliceIterator([]string{}), nil
	}

	var lastErr error
	itWrap := lezhnev74.NewCallbackIterator(
		func() (v string, err error) {
			if lastErr != nil {
				if errors.Is(lastErr, vellum.ErrIteratorDone) {
					lastErr = lezhnev74.EmptyIterator // override the internal error
				}
				return "", lastErr
			}
			term, _ := it.Current()
			if len(term) == 0 {
				return "", lezhnev74.ClosedIterator
			}
			v = string(term)
			lastErr = it.Next()
			return
		},
		func() error {
			return nil
		})

	return itWrap, nil
}

func (i *InvertedIndex[V]) readFooter() (
	fst *vellum.FST,
	segmentSize int64,
	indexOffset int64,
	fstOffset int64,
	err error,
) {

	buf := make([]byte, 4096)
	fstLenSize := int64(8)
	segSize := int64(4)
	indexOffsetSize := int64(8)
	totalSizes := fstLenSize + segSize + indexOffsetSize

	var (
		fstLen int64
	)

	// read the end of the file for parsing footer
	fstat, err := i.file.Stat()
	if err != nil {
		return
	}
	fileSize := fstat.Size()

	if fstat.Size() <= fstLenSize+segSize+indexOffsetSize {
		err = fmt.Errorf("the file size is too small (%d bytes), probably corrupted", fstat.Size())
		return
	}

	p := min(fstat.Size(), int64(len(buf)))
	_, err = i.file.Seek(-p, io.SeekEnd)
	if err != nil {
		err = fmt.Errorf("fst: %w", err)
		return
	}

	n, err := i.file.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("read footer length: %w", err)
		return
	}

	// extract footer ints
	indexOffset = int64(binary.BigEndian.Uint64(buf[int64(n)-indexOffsetSize:]))

	segmentSize = int64(binary.BigEndian.Uint32(buf[int64(n)-indexOffsetSize-segSize : int64(n)-indexOffsetSize]))

	fstLen = int64(binary.BigEndian.Uint64(buf[int64(n)-indexOffsetSize-segSize-fstLenSize : int64(n)-indexOffsetSize-segSize]))
	fstOffset = fileSize - fstLen - fstLenSize - segSize - indexOffsetSize

	// read fst body
	if fstLen > int64(n)-fstLenSize-totalSizes { // do we need to read more bytes?
		_, err = i.file.Seek(-(fstLen + totalSizes), io.SeekEnd)
		if err != nil {
			return
		}

		buf = make([]byte, fstLen)
		_, err = i.file.Read(buf)
		if err != nil {
			return
		}
	} else {
		buf = buf[n-int(fstLen+totalSizes) : int64(n)-totalSizes]
	}

	fst, err = vellum.Load(buf) // todo: use file for mmap io
	if err != nil {
		err = fmt.Errorf("fst: load failed: %w", err)
		return
	}

	return
}

func (i *InvertedIndex[V]) makeTermValuesFetchFunc(
	term string,
	offset, nextOffset uint64,
	minVal, maxVal V,
) (func() ([]V, error), error) {
	var indexLen, n int
	sizeLen := 4
	buf := make([]byte, 4096)
	valuesLen := nextOffset - offset
	var segments []segmentIndexEntry[V]

	// read segment index
	readFrom := int64(-1)
	if int(valuesLen) < len(buf) {
		// whole region fits in the buffer
		buf = buf[:valuesLen]
		readFrom = int64(offset)
	} else {
		// buffer only partially covers the region
		readFrom = int64(nextOffset) - int64(len(buf))
	}

	_, err := i.file.Seek(readFrom, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("reading values index: %w", err)
	}
	n, err = i.file.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("reading values index: %w", err)
	}

	indexLen = int(binary.BigEndian.Uint32(buf[n-sizeLen:]))
	indexOffset := int64(nextOffset) - int64(sizeLen) - int64(indexLen)

	if indexLen > n-sizeLen { // do we need to read more bytes?
		_, err = i.file.Seek(indexOffset, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("reading values index: %w", err)
		}

		buf = make([]byte, indexLen)
		_, err = i.file.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("reading values index: %w", err)
		}
	} else {
		buf = buf[n-sizeLen-indexLen : n-sizeLen]
	}

	segments, err = decodeSegmentsIndex(buf, i.unserializeSegment)
	if err != nil {
		return nil, fmt.Errorf("decoding values index: %w", err)
	}

	// Make a term iterator that scans through segments sequentially
	// makeTermFetchFunc returns a function that can be used in an iterator,
	// upon calling the func it will return slices of sorted term's values
	makeTermFetchFunc := func(term string) func() ([]V, error) {
		var (
			segmentLen int64
			minS, maxS V
		)
		segmentBuf := make([]byte, 4096)
		si := 0

		var retFunc func() ([]V, error)
		retFunc = func() ([]V, error) {
			// validate the current segment
			if si == len(segments) {
				return nil, lezhnev74.EmptyIterator
			}

			if segments[si].Min > maxVal {
				return nil, lezhnev74.EmptyIterator // no further segments are good
			}

			minS = segments[si].Min
			maxS = maxVal
			if si < len(segments)-1 {
				maxS = segments[si+1].Min
			}
			if minVal > maxS || maxVal < minS {
				return nil, lezhnev74.EmptyIterator // no further segments are good
			}

			s := &segments[si]

			// read the segment
			_, err = i.file.Seek(s.Offset, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("read values segment failed: %w", err)
			}

			if si == len(segments)-1 {
				segmentLen = indexOffset - s.Offset // the last segment
			} else {
				segmentLen = segments[si+1].Offset - s.Offset
			}

			if int64(cap(segmentBuf)) < segmentLen {
				segmentBuf = make([]byte, segmentLen)
			} else {
				segmentBuf = segmentBuf[:segmentLen]
			}

			_, err = i.file.Read(segmentBuf)
			if err != nil {
				return nil, fmt.Errorf("read values segment failed: %w", err)
			}

			values, err := i.unserializeSegment(segmentBuf)
			if err != nil {
				return nil, fmt.Errorf("values: decompress fail: %w", err)
			}

			si++

			// finally filter values in place with respect to the min/max scope
			k := 0
			for _, v := range values {
				if v < minVal || v > maxVal {
					continue
				}
				values[k] = v
				k++
			}
			values = values[:k]

			if len(values) == 0 {
				// filtering revealed that this segment has no matching values,
				// continue to the next segment:
				return retFunc()
			}

			return values, nil
		}

		return retFunc
	}

	return makeTermFetchFunc(term), nil
}

func (i *InvertedIndex[V]) getAllTermValues() []V {
	totalNum := 0
	for _, tv := range i.termsValues {
		totalNum += len(tv)
	}

	allValues := make([]V, 0, totalNum/2) // to avoid many allocations, start with 50% of total
	for _, tv := range i.termsValues {
		for _, v := range tv {
			if slices.Contains(allValues, v) {
				continue
			}
			allValues = append(allValues, v)
		}
	}
	slices.Sort(allValues)

	return allValues
}

func (i *InvertedIndex[V]) mapTermValuesToBitmaps(allValues []V) []*roaring.Bitmap {
	tb := make([]*roaring.Bitmap, 0, len(i.termsValues)) // bitmaps per term
	pv := make([]uint32, 0)                              // term value positions for the bitmap

	for _, tv := range i.termsValues {
		pv = pv[:0]
		for _, v := range tv {
			p, _ := slices.BinarySearch(allValues, v)
			pv = append(pv, uint32(p))
		}
		b := roaring.BitmapOf(pv...)
		tb = append(tb, b)
	}

	return tb
}

func (i *InvertedIndex[V]) writeAllValues(values []V) (valuesIndexOffset int64, err error) {
	var n int
	index := make([]segmentIndexEntry[V], 0, len(values)/int(i.segmentSize)+1)

	// write segments
	for k := 0; k < len(values); k += int(i.segmentSize) {

		segmentOffset := i.filePos
		segment := values[k:min(len(values), k+int(i.segmentSize))]
		sbuf, err := i.serializeSegment(segment) // todo: wasted allocation?
		if err != nil {
			return 0, fmt.Errorf("serializing values segment failed: %w", err)
		}

		sl, err := i.file.Write(sbuf)
		if err != nil {
			return 0, err
		}
		i.filePos += int64(sl)

		index = append(index, segmentIndexEntry[V]{
			Offset: segmentOffset,
			Min:    segment[0],
		})
	}

	// write segments index
	indexBuf, err := encodeSegmentsIndex(index, i.serializeSegment)
	if err != nil {
		return 0, fmt.Errorf("encoding segments index failed: %w", err)
	}

	valuesIndexOffset = i.filePos

	binary.BigEndian.PutUint64(i.buf8, uint64(len(indexBuf)))
	n, err = i.file.Write(i.buf8)
	if err != nil {
		return 0, fmt.Errorf("failed writing values index size for: %w", err)
	}
	i.filePos += int64(n)

	n, err = i.file.Write(indexBuf)
	if err != nil {
		return 0, fmt.Errorf("failed writing values index: %w", err)
	}
	i.filePos += int64(n)

	return
}

func (i *InvertedIndex[V]) writeTermsBitmapsAndUpdateFST(bitmaps []*roaring.Bitmap) error {

	// use our existing fst to iterate through terms in the same order as they were ingested
	fst, err := vellum.Load(i.fstBuf.Bytes())
	if err != nil {
		return fmt.Errorf("fst read failed: %w", err)
	}
	i.fstBuf.Reset()

	termsIt, err := fst.Iterator(nil, nil)
	if err != nil {
		return fmt.Errorf("fst iterator failed: %w", err)
	}

	// here we create a new FST with actual bitmap offsets
	// since we can't update the existing FST build with term numbers
	i.fstBuilder, err = vellum.New(i.fstBuf, nil)
	var n int64
	for {
		t, tn := termsIt.Current() // the value is the index of a term
		err = i.fstBuilder.Insert(t, uint64(i.filePos))
		if err != nil {
			return err
		}

		// write the bitmap
		n, err = bitmaps[tn].WriteTo(i.file) // the index of a bitmap is the same as the term in FST
		if err != nil {
			return err
		}
		i.filePos += n

		err = termsIt.Next()
		if err != nil && errors.Is(err, vellum.ErrIteratorDone) {
			break
		} else if err != nil {
			return fmt.Errorf("fst iteration failed: %w", err)
		}
	}
	err = i.fstBuilder.Close()
	if err != nil {
		return err
	}

	return nil
}

// write flushes all in-memory data to the file
func (i *InvertedIndex[V]) write() error {

	err := i.fstBuilder.Close()
	if err != nil {
		return fmt.Errorf("fst: %w", err)
	}

	allValues := i.getAllTermValues()
	termBitmaps := i.mapTermValuesToBitmaps(allValues)
	i.termsValues = nil // free up

	valuesIndexOffset, err := i.writeAllValues(allValues)
	if err != nil {
		return fmt.Errorf("writing all values failed: %w", err)
	}
	allValues = nil // free up

	err = i.writeTermsBitmapsAndUpdateFST(termBitmaps)
	if err != nil {
		return fmt.Errorf("writing bitmaps failed: %w", err)
	}

	// write FST
	fstL, err := i.file.Write(i.fstBuf.Bytes())
	if err != nil {
		return fmt.Errorf("fst: failed writing: %w", err)
	}

	binary.BigEndian.PutUint64(i.buf8, uint64(fstL))
	_, err = i.file.Write(i.buf8)
	if err != nil {
		return fmt.Errorf("fst: failed writing size: %w", err)
	}
	i.fstBuf = nil // free up

	// write the footer
	binary.BigEndian.PutUint32(i.buf4, i.segmentSize)
	_, err = i.file.Write(i.buf4)
	if err != nil {
		return fmt.Errorf("failed writing segment size: %w", err)
	}

	binary.BigEndian.PutUint64(i.buf8, uint64(valuesIndexOffset))
	_, err = i.file.Write(i.buf8)
	if err != nil {
		return fmt.Errorf("failed writing index offset: %w", err)
	}

	return nil
}

func (i *InvertedIndex[V]) readBitmaps(terms []string) ([]*roaring.Bitmap, error) {
	slices.Sort(terms)
	fstIt, err := i.fst.Iterator([]byte(terms[0]), nil)
	if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
		return nil, fmt.Errorf("read fst: %w", err)
	} else if errors.Is(err, vellum.ErrIteratorDone) {
		return nil, nil
	}

	var (
		offset, nextOffset uint64
		bitmapLen          int
		existingTerm       []byte
	)

	bitmaps := make([]*roaring.Bitmap, 0, len(terms))
	buf := make([]byte, 4096)
	//bbuf := bytes.NewBuffer(buf)

	for _, term := range terms {

		// figure out term's bitmap file region
		err = fstIt.Seek([]byte(term))
		if err != nil {
			return nil, fmt.Errorf("fst seek term: %w", err)
		}
		existingTerm, offset = fstIt.Current()
		if slices.Compare(existingTerm, []byte(term)) != 0 {
			continue // the term is not in the index
		}

		err = fstIt.Next()
		if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
			return nil, fmt.Errorf("fst read: %w", err)
		} else if errors.Is(err, vellum.ErrIteratorDone) {
			nextOffset = uint64(i.fstOffset) // the last bitmap abuts FST
		} else {
			_, nextOffset = fstIt.Current()
		}

		// read the bitmap
		_, err = i.file.Seek(int64(offset), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seek bitmap: %w", err)
		}

		bitmapLen = int(nextOffset - offset)
		if cap(buf) >= bitmapLen {
			buf = buf[:bitmapLen]
		} else {
			buf = make([]byte, bitmapLen)
		}

		//_, err = i.file.Read(buf)
		//if err != nil {
		//	return nil, fmt.Errorf("read bitmap: %w", err)
		//}

		b := roaring.New()
		_, err = b.ReadFrom(i.file)

		if err != nil {
			return nil, fmt.Errorf("parse bitmap: %w", err)
		}
		bitmaps = append(bitmaps, b)
	}

	return bitmaps, nil
}

func (i *InvertedIndex[V]) readValuesIndex() (index []segmentIndexEntry[V], err error) {
	buf := make([]byte, 4096)

	_, err = i.file.Seek(i.indexOffset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}

	_, err = i.file.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("reading index len: %w", err)
	}

	indexLen := int64(binary.BigEndian.Uint64(buf[:8]))
	if int64(cap(buf)) >= (indexLen + 8) {
		// buf contains the index completely
		buf = buf[8 : indexLen+8]
	} else {
		// read index required
		buf = make([]byte, indexLen+8)
		_, err = i.file.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("reading index len: %w", err)
		}
		buf = buf[8 : indexLen+8]
	}

	index, err = decodeSegmentsIndex(buf, i.unserializeSegment)
	if err != nil {
		return nil, fmt.Errorf("decoding values index: %w", err)
	}

	// put each segment's len for further simpler reading as (offset + len)
	for j, s := range index {
		if j+1 == len(index) {
			index[j].len = i.indexOffset - s.Offset
		} else {
			index[j].len = index[j+1].Offset - s.Offset
		}
	}

	return
}

func (i *InvertedIndex[V]) selectSegments(index []segmentIndexEntry[V], b *roaring.Bitmap, minVal V, maxVal V) []segmentIndexEntry[V] {
	// Note: preselection returns only segments that potentially can contain relevant values
	// Based on min/max values and the terms bitmap

	// Filter out segments based on bitmap
	k := 0
	for j := 0; j < len(index); j++ {
		if !b.IntersectsWithInterval(uint64(j*int(i.segmentSize)), uint64((j+1)*int(i.segmentSize))) {
			continue
		}
		index[j].startNum = j * int(i.segmentSize)
		index[k] = index[j]
		k++
	}
	index = index[:k]

	// Filter out based on min/max values
	var minSegV, maxSegV V
	k = 0
	for j := 0; j < len(index); j++ {
		minSegV = index[j].Min
		maxSegV = maxVal
		if (j + 1) < len(index) {
			maxSegV = index[j+1].Min
		}
		if minVal >= maxSegV || maxVal < minSegV {
			continue
		}
		index[k] = index[j]
		k++
	}
	index = index[:k]

	return index
}

func (i *InvertedIndex[V]) makeSegmentsFetchFunc(
	segments []segmentIndexEntry[V],
	b *roaring.Bitmap,
	minVal V,
	maxVal V,
) (func() ([]V, error), error) {
	// Make an iterator that scans through segments sequentially
	// Returns a function that can be used in a slice fetching iterator,
	// upon calling the func it will return slices of sorted values
	makeFetchFunc := func() func() ([]V, error) {
		segmentBuf := make([]byte, 4096)
		si := 0

		var retFunc func() ([]V, error)
		retFunc = func() ([]V, error) {
			// validate the current segment
			if si == len(segments) {
				return nil, lezhnev74.EmptyIterator
			}

			s := &segments[si]

			// read the segment
			_, err := i.file.Seek(s.Offset, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("read values segment failed: %w", err)
			}

			if int64(cap(segmentBuf)) < s.len {
				segmentBuf = make([]byte, s.len)
			} else {
				segmentBuf = segmentBuf[:s.len]
			}

			_, err = i.file.Read(segmentBuf)
			if err != nil {
				return nil, fmt.Errorf("read values segment failed: %w", err)
			}

			values, err := i.unserializeSegment(segmentBuf)
			if err != nil {
				return nil, fmt.Errorf("values: decompress fail: %w", err)
			}

			// filter out segment values based on the bitmap
			segmentBitmap := roaring.New()
			segmentBitmap.AddRange(uint64(s.startNum), uint64(s.startNum)+uint64(i.segmentSize))
			segmentBitmap.And(b)
			b.String()
			segmentBitmap.String()

			sit := segmentBitmap.Iterator()
			k := 0
			for sit.HasNext() {
				vi := sit.Next()
				values[k] = values[vi-uint32(s.startNum)]
				k++
			}
			values = values[:k]

			// finally filter out values in place with respect to the min/max scope
			k = 0
			for _, v := range values {
				if v < minVal || v > maxVal {
					continue
				}
				values[k] = v
				k++
			}
			values = values[:k]

			si++

			if len(values) == 0 {
				// filtering revealed that this segment has no matching values,
				// continue to the next segment:
				return retFunc()
			}

			return values, nil
		}

		return retFunc
	}

	return makeFetchFunc(), nil
}

func NewInvertedIndexUnit[V constraints.Ordered](
	filename string,
	segmentSize uint32,
	serializeValues func([]V) ([]byte, error),
	unserializeValues func([]byte) ([]V, error),
) (InvertedIndexWriter[V], error) {
	if segmentSize < 1 {
		return nil, fmt.Errorf("the segment size is too small")
	}

	var err error
	iiw := &InvertedIndex[V]{
		fstBuf:             new(bytes.Buffer),
		buf4:               make([]byte, 4),
		buf8:               make([]byte, 8),
		serializeSegment:   serializeValues,
		unserializeSegment: unserializeValues,
		segmentSize:        segmentSize,
	}

	iiw.file, err = os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	iiw.fstBuilder, err = vellum.New(iiw.fstBuf, nil)

	return iiw, err
}

func OpenInvertedIndex[V constraints.Ordered](
	file string,
	unserializeValues func([]byte) ([]V, error),
) (InvertedIndexReader[V], error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	i := &InvertedIndex[V]{
		file:               f,
		fstBuf:             new(bytes.Buffer),
		buf4:               make([]byte, 4),
		unserializeSegment: unserializeValues,
		cmp:                lezhnev74.OrderedCmpFunc[V],
	}

	fst, segmSize, indexOffset, fstOffset, err := i.readFooter()
	if err != nil {
		return nil, err
	}

	i.fst = fst
	i.segmentSize = uint32(segmSize)
	i.fstOffset = fstOffset
	i.indexOffset = indexOffset

	return i, nil
}
