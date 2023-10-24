package multiple

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestItValidatesDirectoryPermissions(t *testing.T) {
	dirPath, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(dirPath)

	// valid directory
	require.NoError(t, NewIndexDirectory(dirPath))

	// not writable permissions
	require.NoError(t, os.Chmod(dirPath, 0400))
	require.ErrorContains(t, NewIndexDirectory(dirPath), "the directory is not writable")

	// not writable permissions
	require.NoError(t, os.Chmod(dirPath, 0200))
	require.ErrorContains(t, NewIndexDirectory(dirPath), "the directory is not readable")
}
