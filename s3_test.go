// Package s3 brings S3 files handling to afero
package s3

import (
	"bytes"
	"io"
	"math/rand"
	"os"
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

	bucketName := time.Now().UTC().Format("2006-01-02-15-04-05-123456")

	if _, err := s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		t.Fatal("Could not create bucket:", err)
	}

	return NewFs(bucketName, sess)
}

func testWriteReadFile(t *testing.T, fs afero.Fs, name string, size int) {
	t.Logf("Working on %s with %d bytes", name, size)

	{ // First we write the file
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
	testWriteReadFile(t, s3Fs, "/file-20", 20)
	testWriteReadFile(t, s3Fs, "/file-2M", 2*1024*1024)
	testWriteReadFile(t, s3Fs, "/file-200M", 200*1024*1024)
}

func TestFileCreate(t *testing.T) {
	s3Fs := GetFs(t)

	if _, err := s3Fs.Stat("/file1"); err == nil {
		t.Fatal("We should'nt be able to get a file info at this stage")
	}

	if _, err := s3Fs.Create("/file1"); err != nil {
		t.Fatal("Could not create file")
	}

	if stat, err := s3Fs.Stat("/file1"); err != nil {
		t.Fatal("Could not access file")
	} else if stat.Size() != 0 {
		t.Fatal("File should be empty")
	}
}

func TestDirHandle(t *testing.T) {
	s3Fs := GetFs(t)

	if err := s3Fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir:", err)
	}

	if _, err := s3Fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	}

	if dir1, err := s3Fs.Open("/dir1"); err != nil {
		t.Fatal("Could not open dir1 ")
	} else {
		if files, errReaddir := dir1.Readdir(-1); errReaddir != nil {
			t.Fatal("Could not read dir")
		} else if len(files) != 1 || files[0].Name() != "file1" {
			t.Fatal("Listed files are incorrect !")
		}
	}

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
