# S3 Backend for Afero
## About
It provides an [afero filesystem](https://github.com/spf13/afero/) implementation of an [S3](https://aws.amazon.com/s3/) backend.

I'm very opened to any improvement through issues or pull-request that might lead to a better implementation or even
better testing.

## Key points
- Download & upload file streaming
- 80% coverage (all APIs are tested, but not all errors are reproduced)

## Known limitations
- File appending is not supported because S3 doesn't support it, it could be simulated by rewriting entire files
- Chmod / Chtimes are not supported because S3 doesn't support it, it could be simulated through metadata
- Seeking for write is not supported, seeking for read (using ranges) is functional though


## How to use
Note: Errors handling is skipped for brevity but you definitely have to handle it.
```golang

import(
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
  
	s3 "github.com/fclairamb/afero-s3"
)

func main() {
  // You create a session
  sess, _ := session.NewSession(&aws.Config{
    Region:      aws.String(region),
    Credentials: credentials.NewStaticCredentials(keyID, secretAccessKey, ""),
  })

  // Initialize the file system
  s3Fs := s3.NewFs(bucket, sess)

  // And do your thing
  file, _ := fs.OpenFile(name, os.O_WRONLY, 0777)
  file.WriteString("Hello world !")
  file.Close()
}
```

## Thanks

The initial code (which was massively rewritten) comes from:
- [wreulicke's fork](https://github.com/wreulicke/afero-s3)
- Itself forked from [aviau's fork](https://github.com/aviau/).
- Initially proposed as [an afero PR](https://github.com/spf13/afero/pull/90) by [rgarcia](https://github.com/rgarcia) and updated by [aviau](https://github.com/aviau).
