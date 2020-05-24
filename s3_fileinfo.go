// Package s3 brings S3 files handling to afero
package s3

import (
	"os"
	"time"
)

// FileInfo implements os.FileInfo for a file in S3.
type FileInfo struct {
	name        string
	directory   bool
	sizeInBytes int64
	modTime     time.Time
}

// NewFileInfo creates file cachedInfo.
func NewFileInfo(name string, directory bool, sizeInBytes int64, modTime time.Time) FileInfo {
	return FileInfo{
		name:        name,
		directory:   directory,
		sizeInBytes: sizeInBytes,
		modTime:     modTime,
	}
}

// Name provides the base name of the file.
func (fi FileInfo) Name() string {
	return fi.name
}

// Size provides the length in bytes for a file.
func (fi FileInfo) Size() int64 {
	return fi.sizeInBytes
}

// Mode provides the file mode bits. For a file in S3 this defaults to
// 664 for files, 775 for directories.
// In the future this may return differently depending on the permissions
// available on the bucket.
func (fi FileInfo) Mode() os.FileMode {
	if fi.directory {
		return 0755
	}
	return 0664
}

// ModTime provides the last modification time.
func (fi FileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir provides the abbreviation for Mode().IsDir()
func (fi FileInfo) IsDir() bool {
	return fi.directory
}

// Sys provides the underlying data source (can return nil)
func (fi FileInfo) Sys() interface{} {
	return nil
}
