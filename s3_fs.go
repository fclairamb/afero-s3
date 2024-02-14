// Package s3 brings S3 files handling to afero
package s3

import (
	"bytes"
	"errors"
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/afero"
)

// Fs is an FS object backed by S3.
type Fs struct {
	FileProps *UploadedFileProperties // FileProps define the file properties we want to set for all new files
	session   *session.Session        // Session config
	s3API     *s3.S3
	prefix    string
	bucket    string // Bucket name
}

// UploadedFileProperties defines all the set properties applied to future files
type UploadedFileProperties struct {
	ACL          *string // ACL defines the right to apply
	CacheControl *string // CacheControl defines the Cache-Control header
	ContentType  *string // ContentType define the Content-Type header
}

// NewFs creates a new Fs object writing files to a given S3 bucket.
func NewFs(bucket string, session *session.Session, prefix string) *Fs {
	s3Api := s3.New(session)
	return &Fs{
		bucket:  bucket,
		session: session,
		prefix:  prefix,
		s3API:   s3Api,
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
func (Fs) Name() string { return "s3" }

func (fs Fs) GetPath(path string) string {
	prefix := fs.prefix
	if strings.HasPrefix(path, prefix) {
		return path
	}

	bpath := filepath.Clean(fs.prefix)
	return filepath.Clean(filepath.Join(bpath, path))
}

// Create a file.
func (fs Fs) Create(in_name string) (afero.File, error) {
	name := fs.GetPath(in_name)
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

		_, errPut := fs.s3API.PutObject(req)
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
	return file, fs.s3API.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
}

// Mkdir makes a directory in S3.
func (fs Fs) Mkdir(in_name string, perm os.FileMode) error {
	name := fs.GetPath(in_name)
	file, err := fs.OpenFile(fmt.Sprintf("%s/", path.Clean(name)), os.O_CREATE, perm)
	if err == nil {
		err = file.Close()
	}
	return err
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fs Fs) MkdirAll(in_path string, perm os.FileMode) error {
	path := fs.GetPath(in_path)
	return fs.Mkdir(path, perm)
}

// Open a file for reading.
func (fs Fs) Open(in_name string) (afero.File, error) {
	name := fs.GetPath(in_name)
	return fs.OpenFile(name, os.O_RDONLY, 0777)
}

// OpenFile opens a file.
func (fs Fs) OpenFile(in_name string, flag int, _ os.FileMode) (afero.File, error) {
	name := fs.GetPath(in_name)
	file := NewFile(&fs, name)

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

	info, err := file.Stat()

	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return file, nil
	}

	return file, file.openReadStream(0)
}

// Remove a file
func (fs Fs) Remove(in_name string) error {
	name := fs.GetPath(in_name)
	if _, err := fs.Stat(name); err != nil {
		return err
	}
	return fs.forceRemove(name)
}

func (fs Fs) RemoveDir(name string) error {
	println("DELETE")
	name = fs.GetPath(name)
	_, err := fs.s3API.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fmt.Sprintf("%s/", name)),
	})
	return err
}

// forceRemove doesn't error if a file does not exist.
func (fs Fs) forceRemove(in_name string) error {
	name := fs.GetPath(in_name)
	_, err := fs.s3API.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	return err
}

// RemoveAll removes a path.
func (fs Fs) RemoveAll(in_name string) error {
	name := fs.GetPath(in_name)
	s3dir := NewFile(&fs, name)
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
func (fs Fs) Rename(in_oldname, in_newname string) error {
	oldname := fs.GetPath(in_oldname)
	newname := fs.GetPath(in_newname)
	if oldname == newname {
		return nil
	}

	if file, s_err := fs.Stat(oldname); file.IsDir() || s_err != nil {
		children, err := NewFile(&fs, oldname).ReaddirAll()
		if err != nil {
			return err
		}

		for _, child := range children {
			err := fs.Rename(path.Join(in_oldname, child.Name()), path.Join(in_newname, child.Name()))
			if err != nil {
				return err
			}
		}

		//If the stat failed but there are children, this means its a directory without a directory file
		if len(children) > 0 && s_err == nil {
			return nil
		}
	}

	_, err := fs.s3API.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(fs.bucket),
		CopySource: aws.String(fs.bucket + oldname),
		Key:        aws.String(newname),
	})

	if err != nil {
		fmt.Println("Copy Error", in_oldname, err)
		return err
	}
	_, err = fs.s3API.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(oldname),
	})
	if err != nil {
		fmt.Println("Delete Error", err)
	}
	return err
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *os.PathError.
func (fs Fs) Stat(in_name string) (os.FileInfo, error) {
	name := fs.GetPath(in_name)
	if name == "/" {
		return NewFileInfo(name, true, 0, time.Unix(0, 0)), nil
	}

	out, err := fs.s3API.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		var errRequestFailure awserr.RequestFailure
		if errors.As(err, &errRequestFailure) {
			if errRequestFailure.StatusCode() == 404 {
				statDir, errStat := fs.statDirectory(name)
				return statDir, errStat
			}
		}
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	if strings.HasSuffix(name, "/") {
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
	return NewFileInfo(path.Base(name), false, *out.ContentLength, *out.LastModified), nil
}

func (fs Fs) statDirectory(in_name string) (os.FileInfo, error) {
	name := fs.GetPath(in_name)
	nameClean := path.Clean(name)
	out, err := fs.s3API.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(fs.bucket),
		Prefix:  aws.String(strings.TrimPrefix(nameClean, "/")),
		MaxKeys: aws.Int64(1),
	})

	if err != nil {
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}

	if *out.KeyCount == 0 && name != "" && name != fs.prefix {
		return nil, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  os.ErrNotExist,
		}
	}
	return NewFileInfo(path.Base(name), true, 0, time.Unix(0, 0)), nil
}

// Chmod doesn't exists in S3 but could be implemented by analyzing ACLs
func (fs Fs) Chmod(in_name string, mode os.FileMode) error {
	name := fs.GetPath(in_name)
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

	_, err := fs.s3API.PutObjectAcl(&s3.PutObjectAclInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
		ACL:    aws.String(acl),
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

// I couldn't find a way to make this code cleaner. It's basically a big copy-paste on two
// very similar structures.
func applyFileCreateProps(req *s3.PutObjectInput, p *UploadedFileProperties) {
	if p.ACL != nil {
		req.ACL = p.ACL
	}

	if p.CacheControl != nil {
		req.CacheControl = p.CacheControl
	}

	if p.ContentType != nil {
		req.ContentType = p.ContentType
	}
}

func applyFileWriteProps(req *s3manager.UploadInput, p *UploadedFileProperties) {
	if p.ACL != nil {
		req.ACL = p.ACL
	}

	if p.CacheControl != nil {
		req.CacheControl = p.CacheControl
	}

	if p.ContentType != nil {
		req.ContentType = p.ContentType
	}
}
