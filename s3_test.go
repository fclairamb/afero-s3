// Package s3 brings S3 files handling to afero
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	return __getS3Fs(t, nil, nil)
}

func __getS3Fs(t *testing.T, optCfg func(config *aws.Config), optClt func(clt *s3.Client)) *Fs {
	const defaultRegion = "us-east-1"

	creds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""))
	awsCfg := aws.Config{
		Credentials: creds,
		Region:      defaultRegion,
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               "http://localhost:9000",
				SigningRegion:     defaultRegion,
				HostnameImmutable: true,
			}, nil
		}),
	}

	if optCfg != nil {
		optCfg(&awsCfg)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = true
	})

	if optClt != nil {
		optClt(s3Client)
	}

	// Creating a both non-conflicting and quite easy to understand and diagnose bucket name
	bucketName := fmt.Sprintf(
		"%s-%s-%d",
		bucketBase,
		strings.ToLower(t.Name()),
		atomic.AddInt32(&bucketCounter, 1),
	)

	if _, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		t.Fatal("Could not create bucket:", err)
	}

	fs := NewFsFromClient(bucketName, s3Client)

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

	return fs
}

func testWriteFile(t *testing.T, fs afero.Fs, name string, size int) {
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
	fs := GetFs(t)
	testWriteFile(t, fs, "/file-1K", 1024)
	testWriteFile(t, fs, "/file-1M", 1*1024*1024)
	testWriteFile(t, fs, "/file-10M", 10*1024*1024)
	testWriteFile(t, fs, "/file-100M", 100*1024*1024)
}

func TestFsName(t *testing.T) {
	fs := GetFs(t)
	if fs.Name() != "s3" {
		t.Fatal("Wrong name")
	}
}

func TestFileSeekBig(t *testing.T) {
	fs := GetFs(t)
	size := 10 * 1024 * 1024 // 10MB
	name := "file-10M"

	{ // First we write the file
		t.Log("Writing initial file")
		randomReader := NewLimitedReader(rand.New(rand.NewSource(0)), size)

		file, errOpen := fs.OpenFile(name, os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if _, errWrite := io.Copy(file, randomReader); errWrite != nil {
			t.Fatal("Could not write file:", errWrite)
		}

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}

	{
		t.Log("Checking the second half of it")
		randomReader := NewLimitedReader(rand.New(rand.NewSource(0)), size)
		{ // We skip 5MB by reading them
			buffer := make([]byte, 1*1024*1024)
			for i := 0; i < 5; i++ {
				if _, err := randomReader.Read(buffer); err != nil {
					t.Fatal("Cannot read", err)
				}
			}
		}

		file, errOpen := fs.OpenFile(name, os.O_RDONLY, 0777)

		if errOpen != nil {
			t.Fatal("Cannot open", errOpen)
		}

		if _, err := file.Seek(5*1024*1024, io.SeekCurrent); err != nil {
			t.Fatal("Cannot seek:", err)
		}

		if ok, err := ReadersEqual(randomReader, file); !ok || err != nil {
			t.Fatal("Stream are not equal:", err)
		}

		if err := file.Close(); err != nil {
			t.Fatal("Cannot close", err)
		}
	}
}

//nolint: gocyclo, funlen
func TestFileSeekBasic(t *testing.T) {
	fs := GetFs(t)
	req := require.New(t)

	{ // Writing an initial file
		file, err := fs.OpenFile("file1", os.O_WRONLY, 0777)
		req.NoError(err)

		_, err = file.WriteString("Hello world !")
		req.NoError(err)

		req.NoError(file.Close())
	}

	file, errOpen := fs.Open("file1")
	req.NoError(errOpen)

	buffer := make([]byte, 5)

	{ // Reading the world
		if pos, err := file.Seek(6, io.SeekStart); err != nil || pos != 6 {
			t.Fatal("Could not seek:", err)
		}

		if _, err := file.Read(buffer); err != nil {
			t.Fatal("Could not read buffer:", err)
		}

		if string(buffer) != "world" {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}

	{ // Going 3 bytes backwards
		if pos, err := file.Seek(-3, io.SeekCurrent); err != nil || pos != 8 {
			t.Fatal("Could not seek:", err)
		}

		//smallbuf := buffer[0:2]

		if _, err := file.Read(buffer); err != io.EOF {
			t.Fatal("Could not read buffer:", err)
		}

		if string(buffer) != "rld !" {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}

	{ // And then going back to the beginning
		if pos, err := file.Seek(1, io.SeekStart); err != nil || pos != 1 {
			t.Fatal("Could not seek:", err)
		}

		if _, err := file.Read(buffer); err != nil {
			t.Fatal("Could not read buffer:", err)
		}

		if string(buffer) != "ello " {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}

	{ // And from the end
		if pos, err := file.Seek(5, io.SeekEnd); err != nil || pos != 8 {
			t.Fatal("Could not seek:", err)
		}

		if _, err := file.Read(buffer); err != io.EOF {
			t.Fatal("Could not read buffer:", err)
		}

		req.Equal("rld !", string(buffer))
	}

	// Let's close it
	req.NoError(file.Close())

	// And do an other seek
	_, err := file.Seek(10, io.SeekStart)
	req.EqualError(err, "File is closed")
}

func TestReadAt(t *testing.T) {
	fs := GetFs(t)

	{ // Writing an initial file
		file, errOpen := fs.OpenFile("file1", os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if _, err := file.WriteString("Hello world !"); err != nil {
			t.Fatal("Could not write file:", err)
		}

		if err := file.Close(); err != nil {
			t.Fatal("Could not close file:", err)
		}
	}

	{ // Reading a file
		file, errOpen := fs.Open("file1")
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		defer func() {
			if err := file.Close(); err != nil {
				t.Fatal("Could not close file:", err)
			}
		}()

		buffer := make([]byte, 5)
		if _, err := file.ReadAt(buffer, 6); err != nil {
			t.Fatal("Could not perform ReadAt:", err)
		}

		if string(buffer) != "world" {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}
}

func TestWriteAt(t *testing.T) {
	fs := GetFs(t)

	file, errOpen := fs.OpenFile("file1", os.O_WRONLY, 0777)
	if errOpen != nil {
		t.Fatal("Could not open file:", errOpen)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Fatal("Could not close file:", err)
		}
	}()

	if _, err := file.WriteAt([]byte("hello !"), 1); err == nil {
		t.Fatal("We have no way to make this work !")
	}
}

func TestFileCreate(t *testing.T) {
	fs := GetFs(t)

	if _, err := fs.Stat("/file1"); err == nil {
		t.Fatal("We shouldn't be able to get a file cachedInfo at this stage")
	}

	if file, err := fs.Create("/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	if stat, err := fs.Stat("/file1"); err != nil {
		t.Fatal("Could not access file:", err)
	} else if stat.Size() != 0 {
		t.Fatal("File should be empty")
	}

	if err := fs.Remove("/file1"); err != nil {
		t.Fatal("Could not delete file:", err)
	}

	if _, err := fs.Stat("/file1"); err == nil {
		t.Fatal("Should not be able to access file")
	}
}

func TestRemoveAll(t *testing.T) {
	fs := GetFs(t)

	if err := fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir1:", err)
	}

	if err := fs.Mkdir("/dir1/dir2", 0750); err != nil {
		t.Fatal("Could not create dir2:", err)
	}

	if file, err := fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create dir2:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Could not close /dir1/file1 err:", err)
	}

	if err := fs.RemoveAll("/dir1"); err != nil {
		t.Fatal("Could not delete all files:", err)
	}

	if root, err := fs.Open("/"); err != nil {
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
	fs := GetFs(t)
	if err := fs.MkdirAll("/dir3/dir4", 0755); err != nil {
		t.Fatal("Could not perform MkdirAll:", err)
	}

	if _, err := fs.Stat("/dir3/dir4"); err != nil {
		t.Fatal("Could not read dir4:", err)
	}
}

func TestDirHandle(t *testing.T) {
	fs := GetFs(t)

	// We create a "dir1" directory
	if err := fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir:", err)
	}

	// Then create a "file1" file in it
	if file, err := fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	// Opening "dir1" should work
	if dir1, err := fs.Open("/dir1"); err != nil {
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
	if _, err := fs.Open("/dir2"); err == nil {
		t.Fatal("Opening /dir2 should have triggered an error !")
	}
}

func TestFileReaddirnames(t *testing.T) {
	fs := GetFs(t)

	// We create some dirs
	for _, dir := range []string{"/dir1", "/dir2", "/dir3"} {
		if err := fs.Mkdir(dir, 0750); err != nil {
			t.Fatal("Could not create dir:", err)
		}
	}

	root, errOpen := fs.Open("/")
	if errOpen != nil {
		t.Fatal(errOpen)
	}

	{
		dirs, err := root.Readdirnames(2)
		if err != nil {
			t.Fatal(err)
		}
		if len(dirs) != 2 || dirs[0] != "dir1" || dirs[1] != "dir2" {
			t.Fatal("Wrong dirs")
		}
	}

	{
		dirs, err := root.Readdirnames(2)
		if err != nil {
			t.Fatal(err)
		}
		if len(dirs) != 1 || dirs[0] != "dir3" {
			t.Fatal("Wrong dirs")
		}
	}
}

// This test is only here to explain this FS might behave in a strange way
func TestBadConnection(t *testing.T) {
	req := require.New(t)
	fs := __getS3Fs(t, func(config *aws.Config) {
		config.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: "http://broken",
			}, nil
		})
	}, nil)

	// Let's mess-up the config
	// &BrokenEndpointResolver{}
	// Config.Endpoint = aws.String("http://broken")

	t.Run("Read", func(t *testing.T) {
		// We will fail here because we are checking if the file exists and its type
		// before allowing to read it.
		_, err := fs.Open("file")
		req.Error(err)
	})

	t.Run("Write", func(t *testing.T) {
		// We open the file (but actually nothing happens)
		f, err := fs.OpenFile("file", os.O_WRONLY, 0777)
		req.NoError(err)

		// We write something to the s3.uploader that will itself wait for its buffer to be filled
		// before sending the first request.
		_, err = f.WriteString("hello ")
		req.NoError(err)

		// At this point, something will have failed
		req.Error(f.Close())
	})

	// On a "big" file things don't work the same way though
	t.Run("WriteBig", func(t *testing.T) {
		r := NewLimitedReader(rand.New(rand.NewSource(0)), 10*1024*1024)

		f, err := fs.OpenFile("file", os.O_WRONLY, 0777)
		req.NoError(err)

		written, err := io.Copy(f, r)
		req.Error(err)
		// The default AWS SDK buffer size is 5MB (as such, an SDK update might break this test)
		req.Equal(int64(5*1024*1024), written, "Should fail at 5MB")
	})
}

func TestFileStat(t *testing.T) {
	fs := GetFs(t)

	// We create a "dir1" directory
	if err := fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir:", err)
	}

	// Then create a "file1" file in it
	if file, err := fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	if dir1, err := fs.Open("/dir1"); err != nil {
		t.Fatal(err)
	} else {
		if stat, err := dir1.Stat(); err != nil {
			t.Fatal(err)
		} else if stat.Mode() != 0755 {
			t.Fatal("Wrong dir mode")
		}
	}

	if file1, err := fs.Open("/dir1/file1"); err != nil {
		t.Fatal(err)
	} else {
		if stat, err := file1.Stat(); err != nil {
			t.Fatal(err)
		} else if stat.Mode() != 0664 {
			t.Fatal("Wrong file mode")
		}
	}
}

func testCreateFile(t *testing.T, fs afero.Fs, name string, content string) {
	file, err := fs.OpenFile(name, os.O_WRONLY, 0750)
	if err != nil {
		t.Fatal("Could not open file", name, ":", err)
	}
	if _, err := file.WriteString(content); err != nil {
		t.Fatal("Could not write content to file", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal("Could not close file", err)
	}
}

func TestRename(t *testing.T) {
	fs := GetFs(t)

	if errMkdirAll := fs.MkdirAll("/dir1/dir2", 0750); errMkdirAll != nil {
	} else if file, errOpenFile := fs.OpenFile("/dir1/dir2/file1", os.O_WRONLY, 0750); errOpenFile != nil {
		t.Fatal("Couldn't open file:", errOpenFile)
	} else {
		if _, errWriteString := file.WriteString("Hello world !"); errWriteString != nil {
			t.Fatal("Couldn't write:", errWriteString)
		} else if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close:", errClose)
		}
	}

	if errRename := fs.Rename("/dir1/dir2/file1", "/dir1/dir2/file2"); errRename != nil {
		t.Fatal("Couldn't rename file err:", errRename)
	}

	if _, err := fs.Stat("/dir1/dir2/file1"); err == nil {
		t.Fatal("File shouldn't exist anymore")
	}

	if _, err := fs.Stat("/dir1/dir2/file2"); err != nil {
		t.Fatal("Couldn't fetch file cachedInfo:", err)
	}

	// Renaming of a directory isn't tested because it's not supported by afero in the first place
}

func TestFileTime(t *testing.T) {
	fs := GetFs(t)
	name := "/dir1/file1"
	beforeCreate := time.Now().UTC()
	// Well, we have a 1-second precision
	time.Sleep(time.Second)
	testCreateFile(t, fs, name, "Hello world !")
	time.Sleep(time.Second)
	afterCreate := time.Now().UTC()
	var modTime time.Time
	if info, errStat := fs.Stat(name); errStat != nil {
		t.Fatal("Couldn't stat", name, ":", errStat)
	} else {
		modTime = info.ModTime()
	}
	if modTime.Before(beforeCreate) || modTime.After(afterCreate) {
		t.Fatal("Invalid dates", "modTime =", modTime, "before =", beforeCreate, "after =", afterCreate)
	}
	if err := fs.Chtimes(name, time.Now().UTC(), time.Now().UTC()); err == nil {
		t.Fatal("If Chtimes is supported, we should have a check here")
	}
}

func TestChmod(t *testing.T) {
	fs := GetFs(t)
	name := "/dir1/file1"
	testCreateFile(t, fs, name, "Hello world !")
	if err := fs.Chmod(name, 0600); err != nil {
		t.Fatal("Couldn't set file to private", err)
	}
	for _, m := range []os.FileMode{0606, 0604} {
		if err := fs.Chmod(name, m); err != nil {
			/*
				var fail awserr.RequestFailure
				if errors.As(err, &fail) && fail.Code() == "NotImplemented" {
					t.Log("Minio doesn't support this...")
				} else {
					t.Fatal("Problem setting this", err)
				}
			*/
			t.Fatal("Problem setting this", err)
		}
	}
}

func TestChown(t *testing.T) {
	fs := GetFs(t)
	name := "/dir1/file1"
	testCreateFile(t, fs, name, "Hello world !")
	if err := fs.Chown(name, 1000, 1000); err == nil {
		t.Fatal("If Chown is supported, we should have a check here")
	}
}

func TestContentType(t *testing.T) {
	fs := __getS3Fs(t, nil, nil)
	req := require.New(t)

	t.Run("MimeChecks", func(t *testing.T) {
		fileToMime := map[string]string{
			"file.jpg":       "image/jpeg",
			"file.jpeg":      "image/jpeg",
			"file.png":       "image/png",
			"file.txt":       "text/plain; charset=utf-8",
			"file.html":      "text/html; charset=utf-8",
			"file.htm":       "text/html; charset=utf-8",
			"something.else": "application/octet-stream",
			"something":      "application/octet-stream",
		}

		// We write each file
		for fileName, _ := range fileToMime {
			testCreateFile(t, fs, fileName, "content")
		}

		// And we check the resulting content-type
		for fileName, mimeType := range fileToMime {
			resp, err := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String(fs.bucket),
				Key:    aws.String(fileName),
			})
			req.NoError(err)
			req.Equal(mimeType, *resp.ContentType)
		}
	})

	t.Run("Create", func(t *testing.T) {
		_, err := fs.Create("create.png")
		req.NoError(err)

		resp, err := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String("create.png"),
		})
		req.NoError(err)
		req.Equal("image/png", *resp.ContentType)
	})

	t.Run("Custom", func(t *testing.T) {
		fs.FileProps = &UploadedFileProperties{ContentType: aws.String("my-type")}
		defer func() { fs.FileProps = nil }()
		_, err := fs.Create("custom-create")
		req.NoError(err)

		testCreateFile(t, fs, "custom-write", "content")

		for _, name := range []string{"custom-create", "custom-write"} {
			resp, err := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String(fs.bucket),
				Key:    aws.String(name),
			})
			req.NoError(err)
			req.Equal("my-type", *resp.ContentType)
		}
	})
}

func TestFileProps(t *testing.T) {
	fs := __getS3Fs(t, nil, nil)
	req := require.New(t)

	t.Run("CacheControl", func(t *testing.T) {
		cacheControl := "Cache-Control: max-age=300, max-stale=120"
		fs.FileProps = &UploadedFileProperties{
			CacheControl: aws.String(cacheControl),
		}

		// We create a file
		_, err := fs.Create("create")
		req.NoError(err)

		// We write an other one
		testCreateFile(t, fs, "write", "content")

		for _, name := range []string{"create", "write"} {
			resp, err := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String(fs.bucket),
				Key:    aws.String(name),
			})
			req.NoError(err)
			req.Equal(cacheControl, *resp.CacheControl)
		}
	})

}

func TestFileReaddir(t *testing.T) {
	fs := GetFs(t)
	req := require.New(t)

	err := fs.Mkdir("/dir1", 0750)
	req.NoError(err, "Could not create dir1")

	_, err = fs.Create("/dir1/readme.txt")
	req.NoError(err, "could not create file")

	t.Run("WithNoTrailingSlash", func(t *testing.T) {
		dir, err := fs.Open("/dir1")
		req.NoError(err, "could not open /dir1")

		fis, err := dir.Readdir(1)
		req.NoError(err, "could not readdir /dir1")
		req.Len(fis, 1)
	})

	t.Run("WithNoTrailingSlash", func(t *testing.T) {
		dir, err := fs.Open("/dir1/")
		req.NoError(err, "could not open /dir1/")

		fis, err := dir.Readdir(1)
		req.NoError(err, "could not readdir /dir1/")
		req.Len(fis, 1)
	})
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

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	rc := m.Run()

	// rc 0 means we've passed,
	// and CoverMode will be non empty if run with -cover
	if rc == 0 && testing.CoverMode() != "" {
		c := testing.Coverage()
		if c < 0.80 {
			fmt.Printf("Tests passed but coverage failed at %0.2f\n", c)
			rc = -1
		}
	}
	os.Exit(rc)
}

func TestFileInfo(t *testing.T) {
	fi := NewFileInfo("name", false, 1024, time.Now())
	require.Nil(t, fi.Sys())
}
