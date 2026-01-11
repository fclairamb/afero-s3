# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/claude-code) when working with code in this repository.

## Project Overview

afero-s3 is a Go library that provides an AWS S3 backend implementation for the Afero filesystem abstraction (github.com/spf13/afero). It enables transparent S3 storage access through the standard Afero filesystem interface.

## Common Commands

```bash
# Build
go build -v ./...

# Test (requires MinIO running on localhost:9000)
go test -v ./...

# Test with race detection and coverage
go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

# Lint
golangci-lint run

# Start local MinIO for testing
./run_minio.sh
```

## Project Structure

- `s3_fs.go` - Core filesystem implementation (Fs struct implementing afero.Fs)
- `s3_file.go` - File operations and streaming (File struct implementing afero.File)
- `s3_fileinfo.go` - File metadata (FileInfo struct implementing os.FileInfo)
- `s3_test.go` - Comprehensive test suite

## Architecture

### Key Components

1. **Fs** - The filesystem, manages S3 API interactions, bucket configuration, and file properties (ACL, Cache-Control, Content-Type)

2. **File** - Handles streaming read/write operations with:
   - Goroutines for async uploads with pipe-based streaming
   - Byte range requests for reads and seeking
   - S3 multipart uploader for efficient writes

3. **FileInfo** - File metadata with default modes: 0664 for files, 0755 for directories

### S3 Specifics

- Directories are simulated using marker files with trailing "/" (e.g., "dirname/")
- Path normalization uses `path.Clean()` and handles leading slashes
- Uses `WaitUntilObjectExists()` for eventual consistency after creation
- MIME types are automatically detected based on file extension

## Testing

Tests require a local MinIO instance (S3-compatible server):
- Default endpoint: `http://localhost:9000`
- Credentials: `minioadmin`/`minioadmin`
- Tests create unique bucket names per test run
- Coverage enforcement: minimum 80%

## Known Limitations

- File appending/write seeking: Not supported (S3 limitation)
- Chtimes: Not supported (S3 doesn't support custom timestamps)
- Chmod: Limited support (maps to S3 ACLs: private, public-read, public-read-write)
- Chown: Not supported (POSIX-only concept)

## Code Quality

The project uses strict golangci-lint configuration:
- Function length limit: 80 lines / 40 statements
- Cyclomatic complexity: Max 15
- Cognitive complexity: Max 30
- Line length: Max 120 characters

## Conventional Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/) for commit messages and PR titles. This enables automatic changelog generation and semantic versioning via release-please.

### Format

```
<type>[optional scope]: <description>
```

### Types

- `feat`: New feature (triggers minor version bump)
- `fix`: Bug fix (triggers patch version bump)
- `docs`: Documentation only
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `build`: Build system changes
- `ci`: CI configuration changes
- `chore`: Other changes (dependencies, etc.)

### Scopes (optional)

- `fs`: Filesystem implementation
- `file`: File operations
- `fileinfo`: File metadata
- `deps`: Dependencies
- `docs`: Documentation
- `tests`: Test suite
- `ci`: CI/CD configuration

### Examples

```
feat: add support for custom S3 endpoints
fix(fs): handle empty directory listings correctly
docs: update installation instructions
chore(deps): bump aws-sdk-go to v1.50.0
```

### Breaking Changes

Add `!` after type/scope or include `BREAKING CHANGE:` in the footer to trigger a major version bump:

```
feat!: redesign file streaming API
feat(fs)!: change default ACL behavior
```
