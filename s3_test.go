// Package s3 brings S3 files handling to afero
package s3

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
)

func TestCompatibleAferoS3(t *testing.T) {
	var _ afero.Fs = (*Fs)(nil)
	var _ afero.File = (*File)(nil)
}

func TestCompatibleOsFileInfo(t *testing.T) {
	var _ os.FileInfo = (*FileInfo)(nil)
}

var (
	bucketBase          = time.Now().UTC().Format("2006-01-02-15-04-05")
	bucketCounter int32 = 0
)

func GetFs(t *testing.T) afero.Fs {
	sess, errSession := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("eu-west-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})

	if errSession != nil {
		t.Fatal("Could not create session:", errSession)
	}

	s3Client := s3.New(sess)

	// Creating a both non-conflicting and quite easy to understand and diagnose bucket name
	bucketName := fmt.Sprintf(
		"%s-%s-%d",
		bucketBase,
		strings.ToLower(t.Name()),
		atomic.AddInt32(&bucketCounter, 1),
	)

	if _, err := s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		t.Fatal("Could not create bucket:", err)
	}

	fs := NewFs(bucketName, sess)

	// The following cleanup code works fine but testing.T.Cleanup is only available since Go 1.14 and we don't actually
	// need it for now.
	/*
		t.Cleanup(func() {
			if err := fs.RemoveAll("/"); err != nil {
				t.Fatal("Could not cleanup bucket:", err)
				return
			}

			// The minio implementation makes the RemoveAll("/") also delete the simulated S3 bucket, so we *should* but
			// *can't* use the bucket deletion.
			// if _, err := s3Client.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)}); err != nil {
			//   t.Fatal("Could not delete bucket:", err)
			// }
		})
	*/

	return fs
}

func testWriteReadFile(t *testing.T, fs afero.Fs, name string, size int) {
	t.Logf("Working on %s with %d bytes", name, size)

	{ // First we write the file
		t.Log("  Writing file")
		reader1 := NewLimitedReader(rand.New(rand.NewSource(0)), size)

		file, errOpen := fs.OpenFile(name, os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if _, errWrite := io.Copy(file, reader1); errWrite != nil {
			t.Fatal("Could not write file:", errWrite)
		}

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}

	{ // Then we read the file
		t.Log("  Reading file")
		reader2 := NewLimitedReader(rand.New(rand.NewSource(0)), size)

		file, errOpen := fs.OpenFile(name, os.O_RDONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if ok, err := ReadersEqual(file, reader2); !ok || err != nil {
			t.Fatal("Could not equal reader:", err)
		}

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}
}

func TestFileWrite(t *testing.T) {
	s3Fs := GetFs(t)
	testWriteReadFile(t, s3Fs, "/file-1K", 1024)
	testWriteReadFile(t, s3Fs, "/file-1M", 1*1024*1024)
	testWriteReadFile(t, s3Fs, "/file-10M", 10*1024*1024)
	testWriteReadFile(t, s3Fs, "/file-100M", 100*1024*1024)
}

func TestFileCreate(t *testing.T) {
	s3Fs := GetFs(t)

	if _, err := s3Fs.Stat("/file1"); err == nil {
		t.Fatal("We should'nt be able to get a file info at this stage")
	}

	if file, err := s3Fs.Create("/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	if stat, err := s3Fs.Stat("/file1"); err != nil {
		t.Fatal("Could not access file:", err)
	} else if stat.Size() != 0 {
		t.Fatal("File should be empty")
	}

	if err := s3Fs.Remove("/file1"); err != nil {
		t.Fatal("Could not delete file:", err)
	}

	if _, err := s3Fs.Stat("/file1"); err == nil {
		t.Fatal("Should not be able to access file")
	}
}

func TestRemoveAll(t *testing.T) {
	s3Fs := GetFs(t)

	if err := s3Fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir1:", err)
	}

	if err := s3Fs.Mkdir("/dir1/dir2", 0750); err != nil {
		t.Fatal("Could not create dir2:", err)
	}

	if file, err := s3Fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create dir2:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Could not close /dir1/file1 err:", err)
	}

	if err := s3Fs.RemoveAll("/dir1"); err != nil {
		t.Fatal("Could not delete all files:", err)
	}

	if root, err := s3Fs.Open("/"); err != nil {
		t.Fatal("Could not access root:", root)
	} else {
		if files, err := root.Readdir(-1); err != nil {
			t.Fatal("Could not readdir:", err)
		} else if len(files) != 0 {
			t.Fatal("We should not have any files !")
		}
	}
}

func TestMkdirAll(t *testing.T) {
	s3Fs := GetFs(t)
	if err := s3Fs.MkdirAll("/dir3/dir4", 0755); err != nil {
		t.Fatal("Could not perform MkdirAll:", err)
	}

	if _, err := s3Fs.Stat("/dir3/dir4"); err != nil {
		t.Fatal("Could not read dir4:", err)
	}
}

func TestDirHandle(t *testing.T) {
	s3Fs := GetFs(t)

	// We create a "dir1" directory
	if err := s3Fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir:", err)
	}

	// Then create a "file1" file in it
	if file, err := s3Fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	// Opening "dir1" should work
	if dir1, err := s3Fs.Open("/dir1"); err != nil {
		t.Fatal("Could not open dir1:", err)
	} else {
		// Listing files should be OK too
		if files, errReaddir := dir1.Readdir(-1); errReaddir != nil {
			t.Fatal("Could not read dir")
		} else if len(files) != 1 || files[0].Name() != "file1" {
			t.Fatal("Listed files are incorrect !")
		}
	}

	// Opening "dir2" should fail
	if _, err := s3Fs.Open("/dir2"); err == nil {
		t.Fatal("Opening /dir2 should have triggered an error !")
	}
}

// Source: rog's code from https://groups.google.com/forum/#!topic/golang-nuts/keG78hYt1I0
func ReadersEqual(r1, r2 io.Reader) (bool, error) {
	const chunkSize = 8 * 1024 // 8 KB
	buf1 := make([]byte, chunkSize)
	buf2 := make([]byte, chunkSize)
	for {
		n1, err1 := io.ReadFull(r1, buf1)
		n2, err2 := io.ReadFull(r2, buf2)
		if err1 != nil && err1 != io.EOF && err1 != io.ErrUnexpectedEOF {
			return false, err1
		}
		if err2 != nil && err2 != io.EOF && err2 != io.ErrUnexpectedEOF {
			return false, err2
		}
		if (err1 != nil) != (err2 != nil) || !bytes.Equal(buf1[0:n1], buf2[0:n2]) {
			return false, nil
		}
		if err1 != nil {
			return true, nil
		}
	}
}

type LimitedReader struct {
	reader io.Reader
	size   int
	offset int
}

func NewLimitedReader(reader io.Reader, limit int) *LimitedReader {
	return &LimitedReader{
		reader: reader,
		size:   limit,
	}
}

func (r *LimitedReader) Read(buffer []byte) (int, error) {
	maxRead := r.size - r.offset

	if maxRead == 0 {
		return 0, io.EOF
	} else if maxRead < len(buffer) {
		buffer = buffer[0:maxRead]
	}

	read, err := r.reader.Read(buffer)
	if err == nil {
		r.offset += read
	}
	return read, err
}
