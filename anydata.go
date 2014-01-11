// Package anydata provides a toolkit to transparently fetch data files, cache them locally,
// and automatically decompress and/or extract records from them. It does so through the use of
// Fetcher and Wrapper interfaces. The "formats" and "filters" sub-packages include a variety
// of techniques that will parse and extract records and fields and interoperate well.
//
// Current support includes opening files from local paths and the following URL schemes:
//    http:// https:// ftp:// file://
//
// Transparent decompression is enabled for files (including remote URLs) ending in:
//    .gz .bz2 .bzip2 .zip
//
// Extracting files from .tar and .zip archives is also supported through the use of URL fragments
// (#) specifying the archive extraction path. This is supported for the following extensions:
//    .tar .tar.gz .tgz .tar.bz2 .tbz2 .tar.bzip2
//
// Archives referenced multiple times are only downloaded once and re-used as necessary. For
// example, the following 4 resource strings will result in only 2 FTP downloads:
//
//    ftp://ftp.ncbi.nih.gov/gene/DATA/gene2go.gz
//    ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz#names.dmp
//    ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz#nodes.dmp
//    ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz#citations.dmp
//
// To add support for new URL schemes, implement the Fetcher interface and use RegisterFetcher
// before any calls to GetFetcher. You will likely also want to use Put/GetCachedFile to reduce
// network roundtrips as well. To add support for new archive or compression formats, implement
// the Wrapper interface and call RegisterWrapper.
package anydata

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

// Fetcher describes an instance that can be used to retrieve a data set (specified by a
// resource string) from a local/remote data source.
type Fetcher interface {
	// Fetch attempts to connect and/or fetch the resource (possibly asynchronously).
	// For non-file-based Fetchers, this is where API authentication, etc. should be verified.
	Fetch(resource string) error

	// GetReader returns the io.Reader for the resource.
	GetReader() (io.Reader, error)

	// Detect returns true if the resource string specified can be fetched by this instance.
	Detect(resource string) bool
}

// Wrapper describes an instances that can wrap an existing Fetcher with additional
// functionality (such as transparent decompression).
type Wrapper interface {
	// DetectWrap returns true if the pathname (and optional partname) specified suits this Wrapper.
	DetectWrap(pathname, partname string) bool

	// Wrap returns a wrapped Fetcher that decompresses and/or reads the optional partname from f.
	Wrap(f Fetcher, partname string) (Fetcher, error)
}

var (
	fetchers []Fetcher

	// wrappers wrap fetchers in local extraction code
	// i.e. unzip and return internal file from remote .zip url
	wrappers []Wrapper
)

// GetFetcher returns a Fetcher (optionally wrapped by a matching Wrapper) that will work on the
// specified resource string. It returns the last matching Fetcher (Wrapper) in registration order.
func GetFetcher(resource string) (Fetcher, error) {
	var rf Fetcher

	for _, f := range fetchers {
		if f.Detect(resource) {
			rf = f
			break
		}
	}

	if rf == nil {
		return nil, fmt.Errorf("no defined fetchers match '%s'", resource)
	}

	mainpath := resource
	pathpart := ""
	furl, err := url.Parse(resource)
	if err == nil {
		mainpath = furl.Path
		pathpart = furl.Fragment
	} else if strings.Contains(resource, "#") {
		parts := strings.SplitN(resource, "#", 2)
		mainpath = parts[0]
		pathpart = parts[1]
	}
	for _, w := range wrappers {
		if w.DetectWrap(mainpath, pathpart) {
			rf, err = w.Wrap(rf, pathpart)
		}
	}

	return rf, err
}

///////////////////

// A local file fetcher, which detects bare paths and file:// URLs
type localFetcher struct {
	f *os.File
}

func (n *localFetcher) String() string {
	return "Local File"
}

func (n *localFetcher) Detect(resource string) bool {
	furl, err := url.Parse(resource)
	if err == nil && furl.Scheme != "file" {
		return false
	}
	return true
}

func (n *localFetcher) Fetch(resource string) error {
	furl, err := url.Parse(resource)
	if err != nil {
		n.f, err = os.Open(resource)
	} else {
		n.f, err = os.Open(furl.Path)
	}
	return err
}

func (n *localFetcher) GetReader() (io.Reader, error) {
	return n.f, nil
}

///////////////////

func init() {
	// register the default set of fetchers and wrappers

	RegisterFetcher(&localFetcher{})
	RegisterFetcher(&httpFetcher{})
	RegisterFetcher(&ftpFetcher{})

	RegisterWrapper(&bzWrapper{})
	RegisterWrapper(&gzWrapper{})
	RegisterWrapper(&zipWrapper{})
	RegisterWrapper(&tarballWrapper{})
}

// RegisterFetcher adds f to the list of known Fetchers for use by GetFetcher
func RegisterFetcher(f Fetcher) {
	fetchers = append(fetchers, f)
}

// RegisterWrapper adds w to the list of known Wrappers for use by GetFetcher
func RegisterWrapper(w Wrapper) {
	wrappers = append(wrappers, w)
}
