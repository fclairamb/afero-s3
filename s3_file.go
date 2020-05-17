// Package s3 brings S3 files handling to afero
package s3

import (
	"errors"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/afero"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// File represents a file in S3.
// It is not threadsafe.
// nolint: maligned
type File struct {
	fs   *Fs
	name string

	// State of the file being Read and Written
	streamRead          *ReadSeekerEmulator
	streamWrite         io.WriteCloser
	streamWriteCloseErr chan error

	// readdir state
	readdirContinuationToken *string
	readdirNotTruncated      bool
}

var ErrNotImplemented = errors.New("not implemented (2)")

// NewFile initializes an File object.
func NewFile(fs *Fs, name string) *File {
	return &File{
		fs:   fs,
		name: name,
	}
}

// Name returns the filename, i.e. S3 path without the bucket name.
func (f *File) Name() string { return f.name }

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values, as would be returned
// by ListObjects, in directory order. Subsequent calls on the same file will yield further FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case, if
// Readdir returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in
// a single slice. In this case, if Readdir succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdir returns the FileInfo read until that point
// and a non-nil error.
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	if f.readdirNotTruncated {
		return nil, io.EOF
	}
	if n <= 0 {
		return f.ReaddirAll()
	}
	// ListObjects treats leading slashes as part of the directory name
	// It also needs a trailing slash to list contents of a directory.
	name := trimLeadingSlash(f.Name()) + "/"

	// For the root of the bucket, we need to remove any prefix
	if name == "/" {
		name = ""
	}
	output, err := f.fs.s3API.ListObjectsV2(&s3.ListObjectsV2Input{
		ContinuationToken: f.readdirContinuationToken,
		Bucket:            aws.String(f.fs.bucket),
		Prefix:            aws.String(name),
		Delimiter:         aws.String("/"),
		MaxKeys:           aws.Int64(int64(n)),
	})
	if err != nil {
		return nil, err
	}
	f.readdirContinuationToken = output.NextContinuationToken
	if !(*output.IsTruncated) {
		f.readdirNotTruncated = true
	}
	var fis []os.FileInfo
	for _, subfolder := range output.CommonPrefixes {
		fis = append(fis, NewFileInfo(filepath.Base("/"+*subfolder.Prefix), true, 0, time.Time{}))
	}
	for _, fileObject := range output.Contents {
		if hasTrailingSlash(*fileObject.Key) {
			// S3 includes <name>/ in the Contents listing for <name>
			continue
		}

		fis = append(fis, NewFileInfo(filepath.Base("/"+*fileObject.Key), false, *fileObject.Size, *fileObject.LastModified))
	}

	return fis, nil
}

// ReaddirAll provides list of file info.
func (f *File) ReaddirAll() ([]os.FileInfo, error) {
	fileInfos := []os.FileInfo{}
	for {
		infos, err := f.Readdir(100)
		fileInfos = append(fileInfos, infos...)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
	}
	return fileInfos, nil
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if
// Readdirnames returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in
// a single slice. In this case, if Readdirnames succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdirnames returns the names read until that point and
// a non-nil error.
func (f *File) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	names := make([]string, len(fi))
	for i, f := range fi {
		_, names[i] = filepath.Split(f.Name())
	}
	return names, err
}

// Stat returns the FileInfo structure describing file.
// If there is an error, it will be of type *PathError.
func (f *File) Stat() (os.FileInfo, error) {
	return f.fs.Stat(f.Name())
}

// Sync is a noop.
func (f *File) Sync() error {
	return nil
}

// Truncate changes the size of the file.
// It does not change the I/O offset.
// If there is an error, it will be of type *PathError.
func (f *File) Truncate(int64) error {
	return ErrNotImplemented
}

// WriteString is like Write, but writes the contents of string s rather than
// a slice of bytes.
func (f *File) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any.
func (f *File) Close() error {
	// Closing a reading stream
	if f.streamRead != nil {
		// We try to close the Reader
		defer func() {
			f.streamRead = nil
		}()
		if err := f.streamRead.Close(); err != nil {
			return err
		}
	}

	// Closing a writing stream
	if f.streamWrite != nil {
		defer func() {
			f.streamWrite = nil
			f.streamWriteCloseErr = nil
		}()

		// We try to close the Writer
		if err := f.streamWrite.Close(); err != nil {
			return err
		}
		// And more importantly, we wait for the actual writing performed in go-routine to finish.
		// We might have at most 2*5=10MB of data waiting to be flushed before close returns. This
		// might be rather slow.
		err := <-f.streamWriteCloseErr
		close(f.streamWriteCloseErr)
		return err
	}

	// Or maybe we don't have anything to close
	return afero.ErrFileClosed
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (f *File) Read(p []byte) (int, error) {
	if f.streamRead == nil {
		resp, err := f.fs.s3API.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(f.name),
		})
		if err != nil {
			return 0, err
		}
		f.streamRead = &ReadSeekerEmulator{
			reader: resp.Body,
		}
	}
	return f.streamRead.Read(p)
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Read(p)
	return
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// The behavior of Seek on a file opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	// In write mode, this isn't supported
	if f.streamWrite != nil {
		return 0, ErrNotImplemented
	}
	if f.streamRead != nil {
		return f.streamRead.Seek(offset, whence)
	}
	return 0, afero.ErrFileClosed
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(p []byte) (int, error) {
	if f.streamWrite == nil {

		reader, writer := io.Pipe()

		f.streamWriteCloseErr = make(chan error)
		f.streamWrite = writer

		uploader := s3manager.NewUploader(f.fs.session)
		uploader.Concurrency = 1

		go func() {
			_, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(f.fs.bucket),
				Key:    aws.String(f.name),
				Body:   reader,
			})
			f.streamWriteCloseErr <- err
			close(f.streamWriteCloseErr)
		}()
	}
	return f.streamWrite.Write(p)
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(p).
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Write(p)
	return
}

type ReadSeekerEmulator struct {
	reader io.ReadCloser
	offset int64
}

func (s ReadSeekerEmulator) Seek(offset int64, whence int) (int64, error) {
	var nbBytesToRead int64
	switch whence {
	case io.SeekStart:
		nbBytesToRead = offset - s.offset
	case io.SeekCurrent:
		nbBytesToRead = offset
	case io.SeekEnd:
		return 0, ErrNotImplemented
	}

	// Going backward is technically possible (we just have to re-open the stream) but not supported at this stage
	if nbBytesToRead < 0 {
		return 0, ErrNotImplemented
	}

	// This fake-reading algorithm seems clunky
	bufferSize := int64(8192)
	buffer := make([]byte, 0, 8192)
	for i := int64(0); i < nbBytesToRead; {
		toRead := nbBytesToRead - i
		if toRead > bufferSize {
			toRead = bufferSize
		}
		read, err := s.Read(buffer[0:toRead])
		i += int64(read)
		if err != nil {
			return i, err
		}
	}
	return offset, nil
}

func (s ReadSeekerEmulator) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)
	if err == nil {
		s.offset += int64(n)
	}
	return n, err
}

func (s ReadSeekerEmulator) Close() error {
	return s.reader.Close()
}
