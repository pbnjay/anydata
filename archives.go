package anydata

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
)

// A Zip Wrapper for extracting files within .zip archives.
//
// Note that detection and fetching will succeed even if the filename to extract does not exist
// in the .zip archive. This error will surface when GetReader() is called.
type zipWrapper struct {
	wrapped    Fetcher
	insideName string
}

func (n *zipWrapper) String() string {
	return fmt.Sprintf("%s from zip %s", n.insideName, n.wrapped)
}

func (n *zipWrapper) Detect(resource string) bool {
	return false
}

func (n *zipWrapper) DetectWrap(pathname, partname string) bool {
	if partname != "" && strings.HasSuffix(pathname, ".zip") {
		return true
	}

	return false
}

func (n *zipWrapper) Wrap(f Fetcher, partname string) (Fetcher, error) {
	n.wrapped = f
	n.insideName = partname
	return n, nil
}

func (n *zipWrapper) Fetch(resource string) error {
	return n.wrapped.Fetch(resource)
}

func (n *zipWrapper) GetReader() (io.Reader, error) {
	r, err := n.wrapped.GetReader()
	if err != nil {
		return nil, err
	}

	// read all of r into a Zip reader and extract insideName
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	for _, zf := range zr.File {
		if zf.Name == n.insideName {
			return zf.Open()
		}
	}

	return nil, fmt.Errorf("reading '%s' from .zip failed", n.insideName)
}

///////////////////

// A Tarball Wrapper for extracting files within (optionally compressed) .tar archives. It will
// recognize files ending in any the following suffixes:
//   .tar .tar.gz .tgz .tar.bz1 .tbz2 .tar.bzip2
//
// Note that detection and fetching will succeed even if the filename to extract does not exist
// in the .tar archive. This error will surface when GetReader() is called.
type tarballWrapper struct {
	wrapped    Fetcher
	compType   string
	insideName string
}

func (n *tarballWrapper) String() string {
	return fmt.Sprintf("%s from tarball %s", n.insideName, n.wrapped)
}

func (n *tarballWrapper) Detect(resource string) bool {
	return false
}

func (n *tarballWrapper) DetectWrap(pathname, partname string) bool {
	n.compType = ""
	if partname == "" {
		return false
	}

	if strings.HasSuffix(pathname, ".tar") {
		n.compType = "none"
		return true
	}

	if strings.HasSuffix(pathname, ".tar.gz") || strings.HasSuffix(pathname, ".tgz") {
		n.compType = "gzip"
		return true
	}

	if strings.HasSuffix(pathname, ".tar.bz2") || strings.HasSuffix(pathname, ".tbz2") ||
		strings.HasSuffix(pathname, ".tar.bzip2") {
		n.compType = "bzip2"
		return true
	}

	return false
}

func (n *tarballWrapper) Wrap(f Fetcher, partname string) (Fetcher, error) {
	n.wrapped = f
	n.insideName = partname
	return n, nil
}

func (n *tarballWrapper) Fetch(resource string) error {
	return n.wrapped.Fetch(resource)
}

func (n *tarballWrapper) GetReader() (io.Reader, error) {
	r, err := n.wrapped.GetReader()
	if err != nil {
		return nil, err
	}

	switch n.compType {
	case "":
		return nil, fmt.Errorf("unknown tarball error")
	case "gzip":
		r, err = gzip.NewReader(r)
	case "bzip2":
		r = bzip2.NewReader(r)
	}
	if err != nil {
		return nil, err
	}

	tr := tar.NewReader(r)
	for head, err := tr.Next(); err == nil; head, err = tr.Next() {
		if head.Name == n.insideName {
			return tr, nil
		}
	}

	return nil, fmt.Errorf("reading '%s' from .tar failed", n.insideName)
}
