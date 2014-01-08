// Package formats provides record-based data format specification and parsing methods which are
// suitable for automation. When combined with anydata.GetFetcher, this allows a machine-readable
// representation of many different data sources and formats to be scripted succinctly.
package formats

import (
	"fmt"
	"io"
)

// DataFormat represents a format which can be used to transfer data from providers.
type DataFormat interface {
	// Init initializes this instance with attributes from the provided spec. Useful when
	// deserializing JSON or database storage format descriptions. Calling this method is optional.
	Init(spec map[string]string) error

	// Open prepares to read new records from the specified io.Reader. If Init() has not been
	// called, Open will initialize data structures appropriately.
	Open(r io.Reader) error

	// NextRecord returns the next record as a string, or io.EOF and the end of input.
	// This method requires a prior call to Open()
	NextRecord() (string, error)

	// GetFields splits the given record (as from NextRecord into mapped fields. This method does
	// NOT require a prior call to Open()
	GetFields(record string) (map[interface{}]string, error)

	// NextRecordFields is equivalent to calling NextRecord followed by GetFields, but may be more
	// efficient for complex structures. This method requires a prior call to Open()
	NextRecordFields() (map[interface{}]string, error)
}

var (
	formats = make(map[string]DataFormat)
)

// GetDataFormat uses spec["type"] to search registered DataFormats. If a match is found,
// (DataFormat).Init(spec) is called to initialize it before returning.
func GetDataFormat(spec map[string]string) (DataFormat, error) {
	if df, found := formats[spec["type"]]; found {
		df.Init(spec)
		return df, nil
	}
	return nil, fmt.Errorf("No format matches type '%s'", spec["type"])
}

// RegisterFormat adds the named DataFormat to the search list for GetDataFormat
func RegisterFormat(name string, df DataFormat) {
	formats[name] = df
}

func init() {
	RegisterFormat("simple-delimited", &SimpleDelimited{})
	RegisterFormat("tab-delimited", &SimpleDelimited{FieldDelim: "\t", RecordDelim: "\n", rdLen: 1})
}
