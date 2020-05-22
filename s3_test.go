// Package s3 brings S3 files handling to afero
package s3

import (
	"bytes"
	"io/ioutil"
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

func TestFile(t *testing.T) {
	sess, errSession := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("eu-west-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})

	bucketName := time.Now().UTC().Format("2006-01-02-15-04-05-123456")

	if errSession != nil {
		t.Fatal("Could not create session:", errSession)
	}

	s3Client := s3.New(sess)
	_, errCreateBucket := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if errCreateBucket != nil {
		t.Fatal("Could not create bucket:", errCreateBucket)
	}

	s3Fs := NewFs(bucketName, sess)

	if err := s3Fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir:", err)
	}

	fileContent := []byte("File content")

	{ // First we write the file
		file, errOpen := s3Fs.OpenFile("/dir1/file1", os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if _, errWrite := file.Write(fileContent); errWrite != nil {
			t.Fatal("Could not write file:", errWrite)
		}

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}

	{ // Then we read the file
		file, errOpen := s3Fs.OpenFile("/dir1/file1", os.O_RDONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if data, errRead := ioutil.ReadAll(file); errRead != nil {
			t.Fatal("Could not write file:", errRead)
		} else if !bytes.Equal(fileContent, data) {
			t.Fatal("Invalid content")
		}

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}
}
