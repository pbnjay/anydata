anydata
=======

Go toolkit for handling "any" type of data and source which can be turned into
a record/field structure.  This is a fairly important component to any data
warehouse and/or integration project. Although my specialty is bioinformatics
and many of my examples are based in it, these tools are general enough to be
used in many domains.

Documentation and examples at: http://godoc.org/github.com/pbnjay/anydata

Fetchers
--------
Fetchers are used to retrieve data from a remote (or local) data source.
Appropriate Fetcher instances are automatically returned by `GetFetcher` based
on a provided URL string.


 * `HttpFetcher` - A Fetcher for both http:// and https:// URLs.

    Downloaded files are automatically stored in the cache to save time/bandwidth. Supports HTTP Basic Auth within the URL.

 * `FtpFetcher` - A Fetcher for ftp:// URLs.

    Downloaded files are automatically stored in the cache to save time/bandwidth. Uses anonymous authentication by default, or embedded username/password in URL.

 * `LocalFetcher` - A local file Fetcher, which detects bare paths and file:// URLs


Wrappers
--------
Wrappers are used to transparently decompress and/or extract files. They are
automatically applied to Fetchers returned by `GetFetcher` based on the URL
string provided.


 * `TarballWrapper` - A Wrapper for extracting files within (optionally compressed) .tar archives.

    It will recognize files ending in any the following suffixes: `.tar .tar.gz .tgz .tar.bz2 .tbz2 .tar.bzip2`

 * `ZipWrapper` - A Wrapper for extracting files within .zip archives.

 * `BzWrapper` - A decompression wrapper for bzip2'd files.

 * `GzWrapper` - A decompression wrapper for gzip'd files.


TODO List
---------
 - Add unit tests
 - Flesh out more data format parsers
 - More compression formats? (LZMA/7-zip, etc)
 - Other network transfer types? (RPC, aspera, etc)
