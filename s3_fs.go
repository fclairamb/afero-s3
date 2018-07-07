package s3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/spf13/afero"
)

// Fs is an FS object backed by S3.
type Fs struct {
	bucket string
	s3API  s3iface.S3API
}

// NewFs creates a new Fs object writing files to a given S3 bucket.
func NewFs(bucket string, s3API s3iface.S3API) *Fs {
	return &Fs{bucket: bucket, s3API: s3API}
}

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
	if s3Client, ok := fs.s3API.(*s3.S3); ok {
		return file, s3Client.WaitUntilObjectExists(&s3.HeadObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(name),
		})
	}
	return file, err
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
func (fs Fs) Open(name string) (afero.File, error) {
	if _, err := fs.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return fs.OpenFile(name, os.O_CREATE, 0777)
		}
		return (*File)(nil), err
	}
	return NewFile(fs.bucket, name, fs.s3API, fs), nil
}

// OpenFile opens a file.
func (fs Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file := NewFile(fs.bucket, name, fs.s3API, fs)
	if flag&os.O_APPEND != 0 {
		return file, errors.New("S3 is eventually consistent. Appending files will lead to trouble")
	}
	if flag&os.O_CREATE != 0 {
		if _, err := file.WriteString(""); err != nil {
			return file, err
		}
	}
	return file, nil
}

// Remove a file.
func (fs Fs) Remove(name string) error {
	if _, err := fs.Stat(name); err != nil {
		return err
	}
	_, err := fs.s3API.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	return err
}

// ForceRemove doesn't error if a file does not exist.
func (fs Fs) ForceRemove(name string) error {
	_, err := fs.s3API.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	return err
}

// RemoveAll removes a path.
func (fs Fs) RemoveAll(path string) error {
	s3dir := NewFile(fs.bucket, path, fs.s3API, fs)
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
			if err := fs.ForceRemove(fullpath); err != nil {
				return err
			}
		}
	}
	// finally remove the "file" representing the directory
	if err := fs.ForceRemove(s3dir.Name() + "/"); err != nil {
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
	nameClean := filepath.Clean(name)
	out, err := fs.s3API.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(nameClean),
	})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			statDir, err := fs.statDirectory(name)
			return statDir, err
		}
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	} else if err == nil && hasTrailingSlash(name) {
		// user asked for a directory, but this is a file
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			//Err:  errors.New("not a directory"),
			Err: os.ErrNotExist,
		}
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
			//Err:  errors.New("no such file or directory"),
			Err: os.ErrNotExist,
		}
	}
	return NewFileInfo(filepath.Base(name), true, 0, time.Time{}), nil
}

// Chmod is TODO
func (Fs) Chmod(name string, mode os.FileMode) error {
	panic("implement Chmod")
	return nil
}

// Chtimes is TODO
func (Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	panic("implement Chtimes")
	return nil
}
