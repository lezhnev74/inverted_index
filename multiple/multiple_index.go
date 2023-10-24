package multiple

import (
	"fmt"
	"os"
)

// IndexDirectory manages multiple index files in a directory.
// it supports the same read/write API as a single index file
// and also manages concurrent merging process (removing merged files to keep the disk space low)
type IndexDirectory struct {
	directoryPath           string
	currentList, mergedList *filesList
}

func NewIndexDirectory(path string) error {
	// test the directory for read/write permissions.
	s, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("bad index directory: %w", err)
	}

	if !s.IsDir() {
		return fmt.Errorf("the given path is not a directory")
	}

	mode := s.Mode().Perm()

	if mode&0200 == 0 && mode&0020 == 0 && mode&0002 == 0 {
		return fmt.Errorf("the directory is not writable: %s", path)
	}

	if mode&0400 == 0 && mode&0040 == 0 && mode&0004 == 0 {
		return fmt.Errorf("the directory is not readable: %s", path)
	}

	return nil
}
