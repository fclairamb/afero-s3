// Package s3 brings S3 files handling to afero
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/spf13/afero"
)

// https://aws.github.io/aws-sdk-go-v2/docs/migrating/

// Fs is an FS object backed by S3.
type Fs struct {
	FileProps *UploadedFileProperties // FileProps define the file properties we want to set for all new files
	client    *s3.Client
	bucket    string // Bucket name
	RawMode   bool   // Controls path sanitation.
}

// UploadedFileProperties defines all the set properties applied to future files
type UploadedFileProperties struct {
	CacheControl *string // CacheControl defines the Cache-Control header
	ContentType  *string // ContentType define the Content-Type header
	ACL          string  // ACL defines the right to apply
}

// NewFsFromConfig creates a new Fs instance from an AWS Config
// nolint: gocritic
// Disabling linting, because it's OK to copy this object, it's not very frequent
func NewFsFromConfig(bucket string, config aws.Config) *Fs {
	return &Fs{
		bucket: bucket,
		client: s3.NewFromConfig(config),
	}
}

// NewFsFromClient creates a new Fs instance from a S3 client
func NewFsFromClient(bucket string, client *s3.Client) *Fs {
	return &Fs{
		bucket: bucket,
		client: client,
	}
}

// ErrNotImplemented is returned when this operation is not (yet) implemented
var ErrNotImplemented = errors.New("not implemented")

// ErrNotSupported is returned when this operations is not supported by S3
var ErrNotSupported = errors.New("s3 doesn't support this operation")

// ErrAlreadyOpened is returned when the file is already opened
var ErrAlreadyOpened = errors.New("already opened")

// ErrInvalidSeek is returned when the seek operation is not doable
var ErrInvalidSeek = errors.New("invalid seek offset")

// Name returns the type of FS object this is: Fs.
func (*Fs) Name() string { return "s3" }

// Create a file.
func (fs *Fs) Create(name string) (afero.File, error) {
	name = fs.sanitize(name)
	{ // It's faster to trigger an explicit empty put object than opening a file for write, closing it and re-opening it
		req := &s3.PutObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(name),
			Body:   bytes.NewReader([]byte{}),
		}

		if fs.FileProps != nil {
			applyFileCreateProps(req, fs.FileProps)
		}

		// If no Content-Type was specified, we'll guess one
		if req.ContentType == nil {
			req.ContentType = aws.String(mime.TypeByExtension(filepath.Ext(name)))
		}

		_, errPut := fs.client.PutObject(context.Background(), req)
		if errPut != nil {
			return nil, errPut
		}
	}

	file, err := fs.OpenFile(name, os.O_WRONLY, 0750)
	if err != nil {
		return file, err
	}

	// Create(), like all of S3, is eventually consistent.
	// To protect against unexpected behavior, have this method
	// wait until S3 reports the object exists.
	waiter := s3.NewObjectExistsWaiter(fs.client)
	return file, waiter.Wait(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	}, 30*time.Second)
}

// Mkdir makes a directory in S3.
func (fs *Fs) Mkdir(name string, perm os.FileMode) error {
	name = fs.sanitize(name)
	file, err := fs.OpenFile(fmt.Sprintf("%s/", path.Clean(name)), os.O_CREATE, perm)
	if err == nil {
		err = file.Close()
	}
	return err
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fs *Fs) MkdirAll(path string, perm os.FileMode) error {
	return fs.Mkdir(path, perm)
}

// Open a file for reading.
func (fs *Fs) Open(name string) (afero.File, error) {
	name = fs.sanitize(name)
	return fs.OpenFile(name, os.O_RDONLY, 0777)
}

// OpenFile opens a file.
func (fs *Fs) OpenFile(name string, flag int, _ os.FileMode) (afero.File, error) {
	name = fs.sanitize(name)
	file := NewFile(fs, name)

	// Reading and writing is technically supported but can't lead to anything that makes sense
	if flag&os.O_RDWR != 0 {
		return nil, ErrNotSupported
	}

	// Appending is not supported by S3. It's do-able though by:
	// - Copying the existing file to a new place (for example $file.previous)
	// - Writing a new file, streaming the content of the previous file in it
	// - Writing the data you want to append
	// Quite network intensive, if used in abondance this would lead to terrible performances.
	if flag&os.O_APPEND != 0 {
		return nil, ErrNotSupported
	}

	// Creating is basically a write
	if flag&os.O_CREATE != 0 {
		flag |= os.O_WRONLY
	}

	// We either write
	if flag&os.O_WRONLY != 0 {
		return file, file.openWriteStream()
	}

	// otherwise, assume it's read only

	info, err := file.Stat()

	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return file, nil
	}

	return file, nil
}

// Remove a file
func (fs *Fs) Remove(name string) error {
	name = fs.sanitize(name)
	if _, err := fs.Stat(name); err != nil {
		return err
	}
	return fs.forceRemove(name)
}

// forceRemove doesn't error if a file does not exist.
func (fs *Fs) forceRemove(name string) error {
	_, err := fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	return err
}

// RemoveAll removes a path.
func (fs *Fs) RemoveAll(name string) error {
	name = fs.sanitize(name)
	s3dir := NewFile(fs, name)
	fis, err := s3dir.Readdir(0)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		fullpath := path.Join(s3dir.Name(), fi.Name())
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
func (fs *Fs) Rename(oldname, newname string) error {
	oldname = fs.sanitize(oldname)
	newname = fs.sanitize(newname)
	if oldname == newname {
		return nil
	}
	_, err := fs.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(fs.bucket),
		CopySource: aws.String(fs.bucket + "/" + oldname),
		Key:        aws.String(newname),
	})
	if err != nil {
		return err
	}
	_, err = fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(oldname),
	})
	return err
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *os.PathError.
func (fs *Fs) Stat(name string) (os.FileInfo, error) {
	name = fs.sanitize(name)
	if name == "/" {
		return NewFileInfo(name, true, 0, time.Unix(0, 0)), nil
	}
	out, err := fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		errOp := &smithy.OperationError{}
		if errors.As(err, &errOp) {
			if errOp.OperationName == "HeadObject" {
				statDir, errStat := fs.statDirectory(name)
				return statDir, errStat
			}
		}

		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	} else if strings.HasSuffix(name, "/") {
		// user asked for a directory, but this is a file
		return FileInfo{name: name}, nil
	}
	return NewFileInfo(path.Base(name), false, *out.ContentLength, *out.LastModified), nil
}

func (fs *Fs) statDirectory(name string) (os.FileInfo, error) {
	nameClean := path.Clean(name)
	out, err := fs.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(fs.bucket),
		Prefix:  aws.String(strings.TrimPrefix(nameClean, "/")),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	if out.KeyCount == nil || (*out.KeyCount == 0 && name != "") {
		return nil, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  os.ErrNotExist,
		}
	}
	return NewFileInfo(path.Base(name), true, 0, time.Unix(0, 0)), nil
}

// Chmod doesn't exists in S3 but could be implemented by analyzing ACLs
func (fs *Fs) Chmod(name string, mode os.FileMode) error {
	name = fs.sanitize(name)
	var acl string

	otherRead := mode&(1<<2) != 0
	otherWrite := mode&(1<<1) != 0

	switch {
	case otherRead && otherWrite:
		acl = "public-read-write"
	case otherRead:
		acl = "public-read"
	default:
		acl = "private"
	}

	_, err := fs.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
		ACL:    types.ObjectCannedACL(acl),
	})
	return err
}

// Chown doesn't exist in S3 should probably NOT have been added to afero as it's POSIX-only concept.
func (Fs) Chown(string, int, int) error {
	return ErrNotSupported
}

// Chtimes could be implemented if needed, but that would require to override object properties using metadata,
// which makes it a non-standard solution
func (Fs) Chtimes(string, time.Time, time.Time) error {
	return ErrNotSupported
}

// sanitize name if not in RawMode.
func (fs Fs) sanitize(name string) string {
	if fs.RawMode {
		return name
	}
	return sanitize(name)
}

// I couldn't find a way to make this code cleaner. It's basically a big copy-paste on two
// very similar structures.
func applyFileCreateProps(req *s3.PutObjectInput, p *UploadedFileProperties) {
	if p.ACL != "" {
		req.ACL = types.ObjectCannedACL(p.ACL)
	}

	if p.CacheControl != nil {
		req.CacheControl = p.CacheControl
	}

	if p.ContentType != nil {
		req.ContentType = p.ContentType
	}
}

func applyFileWriteProps(input *s3.PutObjectInput, p *UploadedFileProperties) {
	if p.ACL != "" {
		input.ACL = types.ObjectCannedACL(p.ACL)
	}

	if p.CacheControl != nil {
		input.CacheControl = p.CacheControl
	}

	if p.ContentType != nil {
		input.ContentType = p.ContentType
	}
}

// volumePrefixRegex matches the windows volume identifier eg "C:".
var volumePrefixRegex = regexp.MustCompile(`^[[:alpha:]]:`)

// sanitize name to ensure it uses forward slash paths even on Windows
// systems.
func sanitize(name string) string {
	if strings.TrimSpace(name) == "" {
		return name
	}
	name = volumePrefixRegex.ReplaceAllString(name, "")
	name = strings.ReplaceAll(name, "\\", "/")
	hasTrailingSlash := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	if hasTrailingSlash {
		name += "/"
	}
	return name
}
