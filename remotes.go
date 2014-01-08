package anydata

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	goftp "github.com/jlaffaye/goftp"
)

// An HTTP fetcher for both http:// and https:// URLs. Downloaded files are automatically stored
// in the cache to save time/bandwidth. Supports HTTP Basic Auth within the URL.
type HttpFetcher struct {
	data []byte
}

func (n *HttpFetcher) String() string {
	return "HTTP(S) Download"
}

func (n *HttpFetcher) Detect(resource string) bool {
	if strings.HasPrefix(resource, "http://") {
		return true
	}
	if strings.HasPrefix(resource, "https://") {
		return true
	}
	return false
}

func (n *HttpFetcher) Fetch(resource string) error {
	n.data = GetCachedFile(resource)
	if n.data != nil {
		return nil
	}

	furl, err := url.Parse(resource)
	if err != nil {
		return err
	}
	cli := &http.Client{}
	req, err := http.NewRequest("GET", resource, nil)
	if err != nil {
		return err
	}
	if furl.User != nil {
		passwd, _ := furl.User.Password()
		req.SetBasicAuth(furl.User.Username(), passwd)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}

	n.data, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	PutCachedFile(resource, n.data)
	return err
}

func (n *HttpFetcher) GetReader() (io.Reader, error) {
	if n.data == nil || len(n.data) == 0 {
		return nil, fmt.Errorf("Reading failed - did you Fetch()?")
	}

	return bytes.NewReader(n.data), nil
}

///////////////////

// An FTP fetcher for both ftp:// URLs. Downloaded files are automatically stored in the cache to
// save time/bandwidth. Uses anonymous authentication by default, so supply username/password in
// the URL if required.
type FtpFetcher struct {
	data []byte
}

func (n *FtpFetcher) String() string {
	return "FTP Download"
}

func (n *FtpFetcher) Detect(resource string) bool {
	return strings.HasPrefix(resource, "ftp://")
}

func (n *FtpFetcher) Fetch(resource string) error {
	n.data = GetCachedFile(resource)
	if n.data != nil {
		return nil
	}

	furl, err := url.Parse(resource)
	if err != nil {
		return err
	}

	if !strings.Contains(furl.Host, ":") {
		furl.Host = furl.Host + ":21"
	}
	ftp, err := goftp.Connect(furl.Host)
	if err != nil {
		return err
	}
	defer ftp.Quit()

	fusername := "anonymous"
	fpassword := "anythingoes"

	if furl.User != nil {
		passwd, haspass := furl.User.Password()
		if haspass {
			fpassword = passwd
		}
		fusername = furl.User.Username()
	}

	err = ftp.Login(fusername, fpassword)
	if err != nil {
		return err
	}
	defer ftp.Logout()

	resp, err := ftp.Retr(furl.Path)
	if err != nil {
		return err
	}

	n.data, err = ioutil.ReadAll(resp)
	resp.Close()

	PutCachedFile(resource, n.data)
	return err
}

func (n *FtpFetcher) GetReader() (io.Reader, error) {
	if n.data == nil || len(n.data) == 0 {
		return nil, fmt.Errorf("Reading failed - did you Fetch()?")
	}

	return bytes.NewReader(n.data), nil
}
