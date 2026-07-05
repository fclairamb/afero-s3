// Package s3 brings S3 files handling to afero
package s3

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestRealS3 exercises the filesystem against a real, already-existing S3 bucket,
// using whatever AWS credentials are found by the default credential chain (env vars,
// shared config/profile, IAM role, SSO, ...).
//
// It is skipped unless AFERO_S3_TEST_BUCKET is set, so it never runs as part of the
// regular MinIO-backed suite or CI. Anyone with a bucket and credentials can validate
// the library against real S3 with:
//
//	AFERO_S3_TEST_BUCKET=my-bucket go test -run TestRealS3 -v .
//
// Use AWS_PROFILE / AWS_REGION / AWS_ACCESS_KEY_ID+AWS_SECRET_ACCESS_KEY as needed to
// select credentials and region. Everything the test creates lives under a unique,
// randomly-named prefix inside the bucket and is removed again once the test ends;
// the bucket itself is never touched otherwise.
func TestRealS3(t *testing.T) {
	bucket := os.Getenv("AFERO_S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("set AFERO_S3_TEST_BUCKET to an existing bucket you can read/write to run this test against real S3")
	}

	req := require.New(t)
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	req.NoError(err, "could not load AWS config (check your credentials/profile/region)")

	gets := &getCounter{}
	cfg.HTTPClient = &http.Client{Transport: &getCountingTransport{base: http.DefaultTransport, counter: gets}}

	client := s3.NewFromConfig(cfg)
	fs := NewFsFromClient(bucket, client)

	prefix := fmt.Sprintf("afero-s3-test-%d-%d", time.Now().UTC().UnixNano(), rand.Int63()) //nolint:gosec
	t.Cleanup(func() {
		if err := fs.RemoveAll(prefix); err != nil {
			t.Logf("cleanup warning: could not remove test prefix %q: %v", prefix, err)
		}
	})

	smallPath := prefix + "/small.txt"
	bigPath := prefix + "/big.bin"
	bigSize := 12 * 1024 * 1024 // 12MB forces a multipart upload (5MB parts) and many streamed Read() calls.
	bigData := make([]byte, bigSize)
	_, _ = rand.New(rand.NewSource(42)).Read(bigData) //nolint:gosec

	t.Run("MkdirAndStat", func(t *testing.T) {
		testRealS3MkdirAndStat(t, fs, prefix)
	})

	t.Run("SmallFileRoundTrip", func(t *testing.T) {
		testRealS3SmallFileRoundTrip(t, fs, smallPath)
	})

	t.Run("BigFileRoundTrip", func(t *testing.T) {
		testRealS3BigFileRoundTrip(t, fs, bigPath, bigData)
	})

	t.Run("SequentialReadUsesOneGetRequest", func(t *testing.T) {
		testRealS3SequentialRead(t, fs, bigPath, bigSize, gets)
	})

	t.Run("SeekAndReadAt", func(t *testing.T) {
		testRealS3SeekAndReadAt(t, fs, bigPath, bigData)
	})

	t.Run("Readdir", func(t *testing.T) {
		testRealS3Readdir(t, fs, prefix)
	})

	t.Run("Rename", func(t *testing.T) {
		testRealS3Rename(t, fs, smallPath, prefix+"/small-renamed.txt")
	})
}

func testRealS3MkdirAndStat(t *testing.T, fs *Fs, prefix string) {
	req := require.New(t)
	req.NoError(fs.MkdirAll(prefix+"/subdir", 0755))
	info, err := fs.Stat(prefix + "/subdir")
	req.NoError(err)
	req.True(info.IsDir())
}

func testRealS3SmallFileRoundTrip(t *testing.T, fs *Fs, smallPath string) {
	req := require.New(t)
	content := []byte("hello from a real S3 bucket\n")

	file, err := fs.Create(smallPath)
	req.NoError(err)
	n, err := file.Write(content)
	req.NoError(err)
	req.Equal(len(content), n)
	req.NoError(file.Close())

	info, err := fs.Stat(smallPath)
	req.NoError(err)
	req.Equal(int64(len(content)), info.Size())

	readFile, err := fs.Open(smallPath)
	req.NoError(err)
	data, err := io.ReadAll(readFile)
	req.NoError(err)
	req.Equal(content, data)
	req.NoError(readFile.Close())
}

func testRealS3BigFileRoundTrip(t *testing.T, fs *Fs, bigPath string, bigData []byte) {
	req := require.New(t)

	file, err := fs.Create(bigPath)
	req.NoError(err)
	n, err := file.Write(bigData)
	req.NoError(err)
	req.Equal(len(bigData), n)
	req.NoError(file.Close())
}

// testRealS3SequentialRead is a regression check for #938: a full sequential read must
// open a single GetObject stream, not one S3 request per Read() call.
func testRealS3SequentialRead(t *testing.T, fs *Fs, bigPath string, bigSize int, gets *getCounter) {
	req := require.New(t)

	before := gets.load()
	file, err := fs.Open(bigPath) // Open() itself issues one HeadObject (Stat), which this GET counter ignores.
	req.NoError(err)

	buf := make([]byte, 32*1024)
	var total int
	for {
		n, errRead := file.Read(buf)
		total += n
		if errRead == io.EOF {
			break
		}
		req.NoError(errRead)
	}
	req.Equal(bigSize, total)
	req.NoError(file.Close())

	req.Equal(int64(1), gets.load()-before)
}

func testRealS3SeekAndReadAt(t *testing.T, fs *Fs, bigPath string, bigData []byte) {
	req := require.New(t)

	file, err := fs.Open(bigPath)
	req.NoError(err)
	offset := int64(1024 * 1024)
	_, err = file.Seek(offset, io.SeekStart)
	req.NoError(err)
	buf := make([]byte, 4096)
	n, err := file.Read(buf)
	req.NoError(err)
	req.Equal(bigData[offset:offset+int64(n)], buf[:n])
	req.NoError(file.Close())

	file2, err := fs.Open(bigPath)
	req.NoError(err)
	off := int64(2 * 1024 * 1024)
	n2, err := file2.ReadAt(buf, off)
	req.NoError(err)
	req.Equal(bigData[off:off+int64(n2)], buf[:n2])
	req.NoError(file2.Close())
}

func testRealS3Readdir(t *testing.T, fs *Fs, prefix string) {
	req := require.New(t)

	dir, err := fs.Open(prefix)
	req.NoError(err)
	infos, err := dir.Readdir(0)
	req.NoError(err)
	req.NoError(dir.Close())

	names := make(map[string]bool, len(infos))
	for _, info := range infos {
		names[info.Name()] = true
	}
	req.True(names["small.txt"])
	req.True(names["big.bin"])
	req.True(names["subdir"])
}

func testRealS3Rename(t *testing.T, fs *Fs, oldPath, newPath string) {
	req := require.New(t)

	req.NoError(fs.Rename(oldPath, newPath))
	_, err := fs.Stat(oldPath)
	req.Error(err)

	info, err := fs.Stat(newPath)
	req.NoError(err)
	req.False(info.IsDir())

	req.NoError(fs.Remove(newPath))
	_, err = fs.Stat(newPath)
	req.Error(err)
}

// getCounter counts GET requests made to S3, used to assert that a streaming read
// doesn't issue more S3 requests than expected.
type getCounter struct {
	count int64
}

func (c *getCounter) load() int64 {
	return atomic.LoadInt64(&c.count)
}

// getCountingTransport counts outgoing S3 GetObject requests. It ignores requests to
// non-S3 endpoints (e.g. STS or the credential/metadata services) by only counting GETs
// whose path looks like an S3 object path (i.e. carries our test prefix).
type getCountingTransport struct {
	base    http.RoundTripper
	counter *getCounter
}

func (t *getCountingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/big.bin") {
		atomic.AddInt64(&t.counter.count, 1)
	}
	return t.base.RoundTrip(r)
}
