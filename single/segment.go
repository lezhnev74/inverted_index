package single

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/ronanh/intcomp"
	"golang.org/x/exp/constraints"
)

// Segments index
type segmentIndexEntry[V constraints.Ordered] struct {
	Offset int64
	Min    V
	// non-exportable, only used in the reading to remember segment's starting index number
	// which is the number of the first value in the segment regarding to all values in the index
	// thus if only few segments are selected for reading, we know how to map them to a bitmap
	startNum int
}

func compressUint32(items []uint32) ([]byte, error) {
	b := make([]byte, 4)
	encoded := intcomp.CompressUint32(items, nil)
	out := make([]byte, 0, len(encoded)*4)
	for _, u := range encoded {
		binary.BigEndian.PutUint32(b[:4], u)
		out = append(out, b[:4]...)
	}

	return out, nil
}

func decompressUint32(data []byte) (items []uint32, err error) {
	valueInts := make([]uint32, 0)
	for i := 0; i < len(data); i += 4 {
		valueInts = append(valueInts, binary.BigEndian.Uint32(data[i:i+4]))
	}
	items = intcomp.UncompressUint32(valueInts, nil)

	return
}

func compressGob[T any](items []T) ([]byte, error) {
	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	err := enc.Encode(items)
	return w.Bytes(), err
}

func decompressGob[T any](data []byte) (items []T, err error) {
	w := bytes.NewBuffer(data)
	enc := gob.NewDecoder(w)
	err = enc.Decode(&items)
	return
}

func encodeSegmentsIndex[V constraints.Ordered](
	segmentsIndex []segmentIndexEntry[V],
	serializeSegment func([]V) ([]byte, error),
) ([]byte, error) {

	if len(segmentsIndex) == 0 {
		return []byte{}, nil
	}

	/*
			Index Layout:
		   ┌─────────────┬──────────┬─────────┐
		   │OffsetsLen(8)│Offsets(*)│Values(*)│
		   └─────────────┴──────────┴─────────┘
	*/

	sizeLen := 8 // int64
	b := make([]byte, sizeLen)
	offsets := make([]int64, len(segmentsIndex))
	values := make([]V, len(segmentsIndex))

	for k, s := range segmentsIndex {
		offsets[k] = s.Offset
		values[k] = s.Min
	}

	encodedOffsets := intcomp.CompressInt64(offsets, nil)
	encodedValues, err := serializeSegment(values)
	if err != nil {
		return nil, fmt.Errorf("index serialize failed: %w", err)
	}

	// lay out buffer:
	out := make([]byte, 0, sizeLen+sizeLen+len(encodedOffsets)+len(encodedValues))

	binary.BigEndian.PutUint64(b, uint64(len(encodedOffsets)*sizeLen)) // uint64 size
	out = append(out, b...)

	for _, u := range encodedOffsets {
		binary.BigEndian.PutUint64(b, u)
		out = append(out, b...)
	}

	out = append(out, encodedValues...)

	return out, nil
}

func decodeSegmentsIndex[V constraints.Ordered](
	data []byte,
	unserializeSegment func([]byte) ([]V, error),
) (index []segmentIndexEntry[V], err error) {
	if len(data) == 0 {
		return nil, nil
	}

	sizeLen := 8

	offsetsLen := int(binary.BigEndian.Uint64(data[:sizeLen]))

	offsetsBuf := data[sizeLen : offsetsLen+sizeLen]
	offsetsInts := make([]uint64, 0)
	for i := 0; i < len(offsetsBuf); i += sizeLen {
		offsetsInts = append(offsetsInts, binary.BigEndian.Uint64(offsetsBuf[i:i+sizeLen]))
	}
	offsets := intcomp.UncompressInt64(offsetsInts, nil)

	valuesBuf := data[sizeLen+offsetsLen:]
	values, err := unserializeSegment(valuesBuf)
	if err != nil {
		return nil, fmt.Errorf("decode index failed: %w", err)
	}

	for i, offset := range offsets {
		index = append(index, segmentIndexEntry[V]{Offset: offset, Min: values[i]})
	}

	return
}
