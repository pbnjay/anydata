// A really simple cache method set for persistence to JSON and local files.

package anydata

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

type cachedfile struct {
	LocalName string    `json:"local_path"`
	FetchTime time.Time `json:"fetch_timestamp"`
}

var (
	cachePath string
	cached    map[string]cachedfile

	// time to cache data files for
	cacheAge time.Duration
)

// InitCache initializes the cache by loading prior cached dates and filenames from
// <cpath>/cacheinfo.json if it exists, and setting the desired data age (in days).
// If the cpath folder does not exist, it is created.
// If cacheinfo.json cannot be loaded, then an empty cache is created.
func InitCache(cpath string, ageDays int) {
	cachePath = cpath
	if ageDays < 1 {
		ageDays = 1
	}
	cacheAge = time.Duration(ageDays) * 24 * time.Hour
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
