package s3

import (
	"os"

	"github.com/spf13/afero"
)

func testCompatibleAferoS3() {
	var _ afero.Fs = (*Fs)(nil)
	var _ afero.File = (*File)(nil)
}

func testCompatibleOsFileInfo() {
	var _ os.FileInfo = (*FileInfo)(nil)
}
