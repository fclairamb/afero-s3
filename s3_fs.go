// Package s3 brings S3 files handling to afero
package s3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
)

// Fs is an FS object backed by S3.
type Fs struct {
	bucket  string           // Bucket name
	session *session.Session // Session config
	s3API   *s3.S3
}

// NewFs creates a new Fs object writing files to a given S3 bucket.
func NewFs(bucket string, session *session.Session) *Fs {
	s3Api := s3.New(session)
	return &Fs{
		bucket:  bucket,
		session: session,
		s3API:   s3Api,
	}
}

// ErrNotImplemented is returned when this operation is not (yet) implemented
var ErrNotImplemented = errors.New("not implemented")

// ErrNotSupported is returned when this operations is not supported by S3
var ErrNotSupported = errors.New("s3 doesn't support this operation")

// ErrAlreadyOpened is returned when the file is already opened
var ErrAlreadyOpened = errors.New("already opened")

// Name returns the type of FS object this is: Fs.
func (Fs) Name() string { return "Fs" }

// Create a file.
func (fs Fs) Create(name string) (afero.File, error) {
	file, err := fs.Open(name)
	if err != nil {
		return file, err
	}
	// Create(), like all of S3, is eventually consistent.
	// To protect against unexpected behavior, have this method
	// wait until S3 reports the object exists.
	return file, fs.s3API.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
}

// Mkdir makes a directory in S3.
func (fs Fs) Mkdir(name string, perm os.FileMode) error {
	_, err := fs.OpenFile(fmt.Sprintf("%s/", filepath.Clean(name)), os.O_CREATE, perm)
	return err
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fs Fs) MkdirAll(path string, perm os.FileMode) error {
	return fs.Mkdir(path, perm)
}

// Open a file for reading.
// If the file doesn't exist, Open will create the file.
func (fs *Fs) Open(name string) (afero.File, error) {
	if _, err := fs.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return fs.OpenFile(name, os.O_CREATE, 0777)
		}
		return (*File)(nil), err
	}
	return fs.OpenFile(name, os.O_RDONLY, 0777)
}

// OpenFile opens a file.
func (fs *Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file := NewFile(fs, name)

	// Reading and writing is technically supported but can't lead to anything that makes sense
	if flag&os.O_RDWR != 0 {
		return nil, ErrNotSupported
	}

	// Appending is not supported
	if flag&os.O_APPEND != 0 {
		return nil, ErrNotSupported
	}

	// We don't really support anything else than creating a file
	/*
		if flag&os.O_CREATE != 0 {
			if _, err := file.WriteString(""); err != nil {
				return file, err
			}
		}
	*/

	if flag&os.O_WRONLY != 0 {
		return file, file.openWriteStream()
	}

	return file, file.openReadStream()
}

// Remove a file.
func (fs Fs) Remove(name string) error {
	if _, err := fs.Stat(name); err != nil {
		return err
	}
	return fs.forceRemove(name)
}

// forceRemove doesn't error if a file does not exist.
func (fs Fs) forceRemove(name string) error {
	_, err := fs.s3API.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	return err
}

// RemoveAll removes a path.
func (fs *Fs) RemoveAll(path string) error {
	s3dir := NewFile(fs, path)
	fis, err := s3dir.Readdir(0)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		fullpath := filepath.Join(s3dir.Name(), fi.Name())
		if fi.IsDir() {
			if err := fs.RemoveAll(fullpath); err != nil {
				return err
			}
		} else {
			if err := fs.forceRemove(fullpath); err != nil {
				return err
			}
		}
	}
	// finally remove the "file" representing the directory
	if err := fs.forceRemove(s3dir.Name() + "/"); err != nil {
		return err
	}
	return nil
}

// Rename a file.
// There is no method to directly rename an S3 object, so the Rename
// will copy the file to an object with the new name and then delete
// the original.
func (fs Fs) Rename(oldname, newname string) error {
	if oldname == newname {
		return nil
	}
	_, err := fs.s3API.CopyObject(&s3.CopyObjectInput{
		Bucket:               aws.String(fs.bucket),
		CopySource:           aws.String(fs.bucket + oldname),
		Key:                  aws.String(newname),
		ServerSideEncryption: aws.String("AES256"),
	})
	if err != nil {
		return err
	}
	_, err = fs.s3API.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(oldname),
	})
	return err
}

func hasTrailingSlash(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '/'
}

func trimLeadingSlash(s string) string {
	if len(s) > 0 && s[0] == '/' {
		return s[1:]
	}
	return s
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *os.PathError.
func (fs Fs) Stat(name string) (os.FileInfo, error) {
	//nameClean := filepath.Clean(name)
	out, err := fs.s3API.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			statDir, errStat := fs.statDirectory(name)
			return statDir, errStat
		}
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	} else if hasTrailingSlash(name) {
		// user asked for a directory, but this is a file
		return FileInfo{name: name}, nil
		/*
			return FileInfo{}, &os.PathError{
				Op:   "stat",
				Path: name,
				Err:  os.ErrNotExist,
			}
		*/
	}
	return NewFileInfo(filepath.Base(name), false, *out.ContentLength, *out.LastModified), nil
}

func (fs Fs) statDirectory(name string) (os.FileInfo, error) {
	nameClean := filepath.Clean(name)
	out, err := fs.s3API.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(fs.bucket),
		Prefix:  aws.String(trimLeadingSlash(nameClean)),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	if *out.KeyCount == 0 && name != "" {
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  os.ErrNotExist,
		}
	}
	return NewFileInfo(filepath.Base(name), true, 0, time.Time{}), nil
}

// Chmod is TODO
func (Fs) Chmod(name string, mode os.FileMode) error {
	return errors.New("not implemented")
}

// Chtimes is TODO
func (Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return errors.New("not implemented")
}
