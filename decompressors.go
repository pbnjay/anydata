package anydata

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
)

// A Bzip2 decompression wrapper for non-archives.
type bzWrapper struct {
	wrapped Fetcher
}

func (n *bzWrapper) String() string {
	return fmt.Sprintf("bzip2'd %s", n.wrapped)
}

func (n *bzWrapper) Detect(resource string) bool {
	return false
}

// DetectWrap returns true if parthname is empty and pathname ends in .bz2 or .bzip2
func (n *bzWrapper) DetectWrap(pathname, partname string) bool {
	if partname != "" {
		return false
	}

	if strings.HasSuffix(pathname, ".bz2") || strings.HasSuffix(pathname, ".bzip2") {
		return true
	}

	return false
}

func (n *bzWrapper) Wrap(f Fetcher, partname string) (Fetcher, error) {
	n.wrapped = f
	return n, nil
}

func (n *bzWrapper) Fetch(resource string) error {
	return n.wrapped.Fetch(resource)
}

func (n *bzWrapper) GetReader() (io.Reader, error) {
	r, err := n.wrapped.GetReader()
	if err != nil {
		return nil, err
	}

	return bzip2.NewReader(r), nil
}

///////////////////

// A Gzip decompression wrapper for non-archives.
type gzWrapper struct {
	wrapped Fetcher
}

func (n *gzWrapper) String() string {
	return fmt.Sprintf("gzip'd %s", n.wrapped)
}

func (n *gzWrapper) Detect(resource string) bool {
	return false
}

// DetectWrap returns true if parthname is empty and pathname ends in .gz
func (n *gzWrapper) DetectWrap(pathname, partname string) bool {
	if partname == "" && strings.HasSuffix(pathname, ".gz") {
		return true
	}

	return false
}

func (n *gzWrapper) Wrap(f Fetcher, partname string) (Fetcher, error) {
	n.wrapped = f
	return n, nil
}

func (n *gzWrapper) Fetch(resource string) error {
	return n.wrapped.Fetch(resource)
}

func (n *gzWrapper) GetReader() (io.Reader, error) {
	r, err := n.wrapped.GetReader()
	if err != nil {
		return nil, err
	}

	return gzip.NewReader(r)
}
