# S3 Backend for Afero
## About
It provides an [afero filesystem](https://github.com/spf13) implementation of an [S3](https://aws.amazon.com/s3/) backend.

## Key points
- Quite a few tests
- Download & upload file streaming

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
