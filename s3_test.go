// Package s3 brings S3 files handling to afero
package s3

import (
	"os"
	"testing"

	"github.com/spf13/afero"
)

func TestCompatibleAferoS3(t *testing.T) {
	var _ afero.Fs = (*Fs)(nil)
	var _ afero.File = (*File)(nil)
}

func TestCompatibleOsFileInfo(t *testing.T) {
	var _ os.FileInfo = (*FileInfo)(nil)
}
