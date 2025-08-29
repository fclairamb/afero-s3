# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an S3 backend implementation for the Afero filesystem abstraction library. It provides S3-compatible filesystem operations through the afero.Fs interface, allowing applications to interact with S3 buckets as if they were local filesystems.

## Core Architecture

The codebase consists of four main components:

- **s3_fs.go**: Main filesystem implementation (`Fs` struct) that implements `afero.Fs` interface
- **s3_file.go**: File operations implementation (`File` struct) that implements `afero.File` interface  
- **s3_fileinfo.go**: File metadata implementation (`FileInfo` struct) that implements `os.FileInfo` interface
- **s3_test.go**: Comprehensive test suite using MinIO for local testing

Key architectural patterns:
- Streaming I/O for efficient memory usage on large files
- Eventual consistency handling (e.g., `WaitUntilObjectExists` after creation)
- Directory simulation using trailing slashes and object prefixes
- Error wrapping using `os.PathError` to maintain filesystem interface compatibility

## Development Commands

### Building
```bash
go build -v ./...
```

### Testing
Tests require MinIO running locally on port 9000:
```bash
go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
```

### Linting
```bash
golangci-lint run
```

## S3-Specific Limitations

When working with this codebase, be aware of fundamental S3 limitations that affect the design:

- **No file appending**: S3 doesn't support appending to existing files (returns `ErrNotSupported`)
- **No read-write mode**: Files cannot be opened for both reading and writing simultaneously
- **Limited chmod**: Only basic ACL mapping (private, public-read, public-read-write)
- **No chtimes/chown**: Not supported by S3 object model

## Testing Infrastructure

Tests use MinIO (S3-compatible local server) configured in GitHub Actions. Test buckets are created with timestamp-based names to avoid conflicts. The test helper `__getS3Fs()` sets up a connection to `localhost:9000` with static credentials.

## Dependencies

- **github.com/aws/aws-sdk-go**: AWS SDK for S3 operations
- **github.com/spf13/afero**: Filesystem abstraction interface
- **github.com/stretchr/testify**: Testing utilities