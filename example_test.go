package anydata_test

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/pbnjay/anydata"
)

// List matching lines from a species taxonomy inside a remote tarball.
func Example_usage() {

	// get a Fetcher for names.dmp in the the NCBI Taxonomy tarball
	taxNames := "ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz#names.dmp"
	ftch, err := anydata.GetFetcher(taxNames)
	if err != nil {
		panic(err)
	}

	// download the tarball (if necessary)
	err = ftch.Fetch(taxNames)
	if err != nil {
		panic(err)
	}

	// get an io.Reader to read from names.dmp
	rdr, err := ftch.GetReader()
	if err != nil {
		panic(err)
	}

	// print every line containing "scientific name"
	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "scientific name") {
			fmt.Println(line)
		}
	}
	// Output:
	// 1       |       root    |               |       scientific name |
	// 2       |       Bacteria        |       Bacteria <prokaryote>   |       scientific name |
	// 6       |       Azorhizobium    |               |       scientific name |
	// 7       |       Azorhizobium caulinodans        |               |       scientific name |
	// 9       |       Buchnera aphidicola     |               |       scientific name |
	// 10      |       Cellvibrio      |               |       scientific name |
	// 11      |       [Cellvibrio] gilvus     |               |       scientific name |
	// 13      |       Dictyoglomus    |               |       scientific name |
	// 14      |       Dictyoglomus thermophilum       |               |       scientific name |
	// 16      |       Methylophilus   |               |       scientific name |
	// 17      |       Methylophilus methylotrophus    |               |       scientific name |
	// ...
}
