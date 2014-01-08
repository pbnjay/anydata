// Package anydata provides a toolkit to transparently fetch data files, cache
// them locally, and automatically decompress and/or extract from them. It does so through the
// use of Fetcher and Wrapper interfaces. See the subdirectory formats for a variety of tools
// that will parse and extract records and fields using these Fetchers.
//
// Current support includes opening files from HTTP(S) and FTP URLs, in addition to local files.
// It will transparently decompress gzip and bzip2 files, and can use '#'-denoted fragments to
// extract specific files from .tar and .zip archives. Some example resource strings include:
//
//    ftp://ftp.ncbi.nih.gov/gene/DATA/gene2go.gz
//    ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz#names.dmp
//
package anydata

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
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

type Wrapper interface {
	// DetectWrap returns true if the pathname (and optional partname) specified suits this Wrapper.
	DetectWrap(pathname, partname string) bool

	// Wrap returns a wrapped Fetcher that decompresses and/or reads the optional partname from f.
	Wrap(f Fetcher, partname string) (Fetcher, error)
}

type cachedfile struct {
	LocalName string    `json:"local_path"`
	FetchTime time.Time `json:"fetch_timestamp"`
}

var (
	cachePath string
	cached    map[string]cachedfile
	fetchers  []Fetcher

	// wrappers wrap fetchers in local extraction code
	// i.e. unzip and return internal file from remote .zip url
	wrappers []Wrapper

	// time to cache data files for
	cacheAge time.Duration
)

// InitCache initializes the cache by loading prior cached dates and filenames from
// <cpath>/cacheinfo.json if it exists, and setting the desired data age (in days).
// If the cpath folder does not exist, it is created.
// If cacheinfo.json cannot be loaded, then an empty cache is created.
func InitCache(cpath string, age_days int) {
	cachePath = cpath
	if age_days < 1 {
		age_days = 1
	}
	cacheAge = time.Duration(age_days) * 24 * time.Hour
	cached = make(map[string]cachedfile)

	// create cachePath if it doesn't exist
	os.Mkdir(cachePath, 0777)

	f, err := os.Open(path.Join(cachePath, "cacheinfo.json"))
	if err != nil {
		return
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}

	json.Unmarshal(data, &cached)
}

// GetCachedFile returns the contents of a file (identified by resource) from the cache.
// If the resource is too old or does not exist, returns nil.
func GetCachedFile(resource string) []byte {
	if cached == nil {
		InitCache("cache", 7)
	}

	// if its an archive, strip off the fragment
	// (can't use url.Parse cause it may not be a URL...)
	rparts := strings.SplitN(resource, "#", 2)

	if cinfo, found := cached[rparts[0]]; found {
		if time.Now().Sub(cinfo.FetchTime) > cacheAge {
			log.Printf("Cached copy is too old (%dh)\n", time.Now().Sub(cinfo.FetchTime)/time.Hour)
			return nil
		}

		// cached copy is recent, use it instead of fetching
		f, err := os.Open(path.Join(cachePath, cinfo.LocalName))
		if err == nil {
			data, err := ioutil.ReadAll(f)
			f.Close()

			if err == nil {
				return data
			}
		}
	}
	return nil
}

// PutCachedFile saves the contents of a file (identified by resource) to the cache.
func PutCachedFile(resource string, data []byte) {
	// if its an archive, strip off the fragment
	// (can't use url.Parse cause it may not be a URL...)
	rparts := strings.SplitN(resource, "#", 2)

	// sanitize the filename into an md5 hash, and write to local cache dir
	temphash := md5.New()
	io.WriteString(temphash, rparts[0])
	tempname := fmt.Sprintf("%x", temphash.Sum(nil))
	f, err := os.OpenFile(path.Join(cachePath, tempname), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println(err.Error())
		return
	}
	f.Write(data)
	f.Close()

	// add the cache entry and serialize to disk immediately
	cached[rparts[0]] = cachedfile{LocalName: tempname, FetchTime: time.Now()}
	cdata, err := json.Marshal(cached)
	if err != nil {
		log.Println(err.Error())
		return
	}

	f, err = os.OpenFile(path.Join(cachePath, "cacheinfo.json"), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println(err.Error())
		return
	}
	f.Write(cdata)
	f.Close()

}

// GetFetcher returns a Fetcher that will work on the specified resource string. It returns the
// last matching Fetcher/Wrapper in registration order.
func GetFetcher(resource string) (Fetcher, error) {
	var rf Fetcher

	for _, f := range fetchers {
		if f.Detect(resource) {
			rf = f
			break
		}
	}

	if rf == nil {
		return nil, fmt.Errorf("No defined fetchers match '%s'", resource)
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
type LocalFetcher struct {
	f *os.File
}

func (n *LocalFetcher) String() string {
	return "Local File"
}

func (n *LocalFetcher) Detect(resource string) bool {
	furl, err := url.Parse(resource)
	if err == nil && furl.Scheme != "file" {
		return false
	}
	return true
}

func (n *LocalFetcher) Fetch(resource string) error {
	furl, err := url.Parse(resource)
	if err != nil {
		n.f, err = os.Open(resource)
	} else {
		n.f, err = os.Open(furl.Path)
	}
	return err
}

func (n *LocalFetcher) GetReader() (io.Reader, error) {
	return n.f, nil
}

///////////////////

func init() {
	// register the default set of fetchers and wrappers

	RegisterFetcher(&LocalFetcher{})
	RegisterFetcher(&HttpFetcher{})
	RegisterFetcher(&FtpFetcher{})

	RegisterWrapper(&BzWrapper{})
	RegisterWrapper(&GzWrapper{})
	RegisterWrapper(&ZipWrapper{})
	RegisterWrapper(&TarballWrapper{})
}

// RegisterFetcher adds f to the list of known Fetchers for use by GetFetcher
func RegisterFetcher(f Fetcher) {
	fetchers = append(fetchers, f)
}

// RegisterWrapper adds w to the list of known Wrappers for use by GetFetcher
func RegisterWrapper(w Wrapper) {
	wrappers = append(wrappers, w)
}
