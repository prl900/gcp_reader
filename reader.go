package gcp_storage

import (
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

type BuffReader struct {
	ctx    context.Context
	obj    *storage.ObjectHandle
	rc     io.ReadCloser
	buf    []byte
	offset int
	Log    string
}

func NewBuffReader(bucketName, objectName string, bufSize int) (*BuffReader, error) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)

	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return &BuffReader{
		ctx:    ctx,
		obj:    obj,
		rc:     rc,
		buf:    make([]byte, bufSize),
		offset: -1,
	}, nil
}

func (ra *BuffReader) ReadAt(b []byte, off int64) (int, error) {
	ra.Log += fmt.Sprintf("ReadAt called: length slice=%d, offset=%d", len(b), off)

	if ra == nil {
		return 0, fmt.Errorf("invalid")
	}

	s := int(off) - ra.offset
	e := (int(off) + len(b)) - ra.offset
	if ra.offset >= 0 && s >= 0 && e <= len(ra.buf) {
		ra.Log += fmt.Sprintf("Served from buffer!\n")
		copy(b, ra.buf[s:e])
		return e - s, nil
	}

	if len(b) < len(ra.buf) {
		ra.Log += fmt.Sprintf("It would fit but we don't have it...\n")
		rc, err := ra.obj.NewRangeReader(ra.ctx, off, int64(len(ra.buf)))
		defer rc.Close()
		if err != nil {
			return 0, err
		}

		n, err := io.ReadFull(rc, ra.buf)
		if err != nil {
			return 0, err
		}
		if n != len(ra.buf) {
			return n, fmt.Errorf("couldn't read enough")
		}
		ra.offset = int(off)
		copy(b, ra.buf[:len(b)])

		return len(b), err

	}

	ra.Log += fmt.Sprintf("Does not fit in our buffer...\n")
	rc, err := ra.obj.NewRangeReader(ra.ctx, off, int64(len(b)))
	defer rc.Close()
	if err != nil {
		return 0, err
	}

	return io.ReadFull(rc, b)
}

func (ra *BuffReader) Read(b []byte) (int, error) {
	if ra == nil {
		return 0, fmt.Errorf("invalid")
	}

	return ra.rc.Read(b)
}

func (ra *BuffReader) Close() error {
	return ra.rc.Close()
}
