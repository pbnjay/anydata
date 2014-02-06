// Package formats provides record-based data format specification and parsing methods which are
// suitable for automation. When combined with anydata.GetFetcher, this allows a machine-readable
// representation of many different data sources and formats to be scripted succinctly.
//
// Because data formats vary widely, these implementations are highly configurable using a simple
// and generic map[string]string specification. The currently defined data formats and their
// configurable options are:
//
//    "tab-delimited"
//       Tab ("\t") separated fields and newline ("\n") separated records. No quotes,
//       escapes, or comments are supported. A future implementation will be optimized.
//       No configurable options.
//
//    "simple-delimited"
//       A simple format with string-delimited records and fields. No quotes, escapes,
//       or comments are supported.
//       Options: "fields" = the field separator string (default "\t")
//                "records = the record separator string (default "\n")
//
//    "csv" (WIP)
//       A format providing RFC 4180 parsing (as provided by encoding/csv). It supports
//       quotes, escapes, and line-based comments.
//       Options: "fields"     = the field separator character (default ",")
//                "comments"   = the comment start character (default none)
//                "num_fields" = integer number of fields per record for verification
//                               (default none = infer from first record)
//
//    "fixed" (WIP)
//       A simple fixed-width format where fields start at pre-defined character column
//       boundaries and records are separated by newlines ("\n").
//       Options: "offsets" = Comma-separated string list of 0-based string offsets.
//
// To support new data formats, simply implement the DataFormat interface and call
// RegisterFormat before using GetDataFormat.
//
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

	// Open prepares to read new records from the specified io.Reader.
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

	// HasVariableFields returns false if all records should have the same number of fields
	HasVariableFields() bool
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
	return nil, fmt.Errorf("no format matches type '%s'", spec["type"])
}

// RegisterFormat adds the named DataFormat to the search list for GetDataFormat
func RegisterFormat(name string, df DataFormat) {
	formats[name] = df
}

func init() {
	RegisterFormat("tab-delimited", &simpleDelimited{FieldDelim: "\t", RecordDelim: "\n", rdLen: 1})
	RegisterFormat("simple-delimited", &simpleDelimited{})
	RegisterFormat("csv", &commaSeparated{})
	RegisterFormat("fixed", &fixedWidth{})
}
