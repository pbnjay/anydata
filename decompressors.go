package anydata

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
)

// A Bzip2 decompression wrapper for non-archives.
type BzWrapper struct {
	wrapped Fetcher
}

func (n *BzWrapper) String() string {
	return fmt.Sprintf("bzip2'd %s", n.wrapped)
}

func (n *BzWrapper) Detect(resource string) bool {
	return false
}

// DetectWrap returns true if parthname is empty and pathname ends in .bz2 or .bzip2
func (n *BzWrapper) DetectWrap(pathname, partname string) bool {
	if partname != "" {
		return false
	}

	if strings.HasSuffix(pathname, ".bz2") || strings.HasSuffix(pathname, ".bzip2") {
		return true
	}

	return false
}

func (n *BzWrapper) Wrap(f Fetcher, partname string) (Fetcher, error) {
	n.wrapped = f
	return n, nil
}

func (n *BzWrapper) Fetch(resource string) error {
	return n.wrapped.Fetch(resource)
}

func (n *BzWrapper) GetReader() (io.Reader, error) {
	r, err := n.wrapped.GetReader()
	if err != nil {
		return nil, err
	}

	return bzip2.NewReader(r), nil
}

///////////////////

// A Gzip decompression wrapper for non-archives.
type GzWrapper struct {
	wrapped Fetcher
}

func (n *GzWrapper) String() string {
	return fmt.Sprintf("gzip'd %s", n.wrapped)
}

func (n *GzWrapper) Detect(resource string) bool {
	return false
}

// DetectWrap returns true if parthname is empty and pathname ends in .gz
func (n *GzWrapper) DetectWrap(pathname, partname string) bool {
	if partname == "" && strings.HasSuffix(pathname, ".gz") {
		return true
	}

	return false
}

func (n *GzWrapper) Wrap(f Fetcher, partname string) (Fetcher, error) {
	n.wrapped = f
	return n, nil
}

func (n *GzWrapper) Fetch(resource string) error {
	return n.wrapped.Fetch(resource)
}

func (n *GzWrapper) GetReader() (io.Reader, error) {
	r, err := n.wrapped.GetReader()
	if err != nil {
		return nil, err
	}

	return gzip.NewReader(r)
}
