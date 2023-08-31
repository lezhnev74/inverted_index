# Inverted Index

[![Go package](https://github.com/lezhnev74/inverted_index/actions/workflows/workflow.yml/badge.svg)](https://github.com/lezhnev74/inverted_index/actions/workflows/workflow.yml)

This repository contains code to manage inverted indexes. It introduces a file format to store and read them
efficiently.
The inverted index is a map of string terms to "postings" which are some ordered values (usually doc ids or timestamps).
The main use-case for this lib is to be used for storing inverted indexes of log files and that led to certain
compromises and design decisions. To name a few: the indexes are append only (no modifications are allowed, including
removal). Once the source log file is removed so is the index file. This supported the idea of keeping one index per
source log file. My [blog post](https://lessthan12ms.com/inverted-index.html) explains more ideas behind this.

## Installation And Usage

```
go get github.com/lezhnev74/inverted_index
```

### Working With Single Index File

Usage is simple, there is two phases: writing and reading. Also refer to tests files for more examples.

```go
// Phase-1: Writing To Index

// Since the index can work with different posting types, we have to provide two function for serialization and deserialization.
// The lib comes with an implementation for uint32 (used in case of timestamps as postings). Otherwise, write your own.
compressPostings := CompressUint32
decompressPostings := DecompressUint32
segmentSize := 1000 // internal size of postings segments, each segment can be read individually thus reducing I/O
indexWriter, err := NewInvertedIndexUnit(filename, segmentSize, compressPostings, decompressPostings)

// while you index your files, you put data to the index in order
// the writer accumulates data in memory
for term, postings := range indexingSource {
err = indexWriter.Put(term, postings)
}

// at this step the index file is finalized and closed
err = indexWriter.Close()

```

```go
// Phase-2: Reading Index

// Here we also need to provide the function to decompress posings from the file
indexReader, err := OpenInvertedIndex(filename, decompressPostings)

// We can read terms only from the index:
it, err := indexReader.ReadTerms()
allTerms := lezhnev74.ToSlice(it) // use as a slice or iterate individually

// We can read values for given terms and given constraints:
itValues, err := indexReader.ReadValues(allTerms[2:4], 0, math.MaxUint32) // or use ReadAllValues()

// use the iterators and close it
err = itValues.Close()
```

### Maintaining Multiple Index Files

As you keep indexing the source log files, to keep memory footprint low, we write many index files. Eventually we want
to read data from those as if it was a single index.

```go
mr, err := NewMultipleValuesReader(
[]string{filename1, filename2}, // individual index files
decompressPostings,
[]string{"term1", "term2"}, // select terms to read
0,                          // min posting
999999,                     // max posting
)

// The API is the same as for a single index file explained above.

```

This lib offers means to merge smaller index files to a bigger one to preserve disk space.

```go
err = MergeIndexes(existingIndexFiles, newIndexFile, segmentSize, compressPostings, decompressPostings)

// you can model your merging policies accordingly.

```

## Design Details

To preserve disk space the index format applies a few compressing techniques:

- [FST](https://blog.burntsushi.net/transducers/) data structure to store terms
- all postings are stored together in segments, each segment is compressed with a provided user function (
  see `CompressUint32` as an example)
- to select term's postings it stores bitmaps for each term so selection works as application of a bitmap to all
  postings

These ideas come from the Lucene design. Compression here works only as good as the data that comes in. Highly random
data will be less compressible (applies to both terms and postings).

Here is the file format used for the index. Made with [asciiflow.com](https://asciiflow.com/).
```
		File Layout:

                            ┌─────*──────┬──8───┬───────4───────┬────4─────┬────────8────────┐
                            │MinMaxValues│FSTLen│MinMaxValuesLen│SegmentLen│ValuesIndexOffset│
                            └────────────┴──────┴───────────────┴──────────┴─────────────────┘
                             \          ____________________________________________________/
                              \        /
            ┌──*───┬───*───┬─*─┬──*───┐
            │Values│Bitmaps│FST│Footer│
            └──────┴───────┴───┴──────┘
          /         \__________________________
         |                                     \
         ┌───*────┬─────┬───*────┬───8────┬──*──┐
         │Segment1│ ... │SegmentN│IndexLen│Index│
         └────────┴─────┴────────┴────────┴─────┘

```

## Contribute

Open an issue and let's continue there.
