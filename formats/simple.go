package formats

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// The SimpleDelimited DataFormat describes a basic format with no comments, escapes, or quotes and
// simple string-based field and record delimiters ("\t" and "\n", respectively by default).
// See CommaSeparated if you need quoting and escaping features.
type SimpleDelimited struct {
	FieldDelim  string
	RecordDelim string
	rdLen       int
	reader      io.Reader
	scanner     *bufio.Scanner
}

// Init initializes the field and record delimiters using spec["fields"] and spec["records"].
func (f *SimpleDelimited) Init(spec map[string]string) error {
	// defaults
	f.FieldDelim = "\t"
	f.RecordDelim = "\n"

	if spec != nil {
		if fd, found := spec["fields"]; found {
			f.FieldDelim = fd
		}
		if rd, found := spec["records"]; found {
			f.RecordDelim = rd
		}
	}

	f.rdLen = len([]byte(f.RecordDelim))
	return nil
}

func (f *SimpleDelimited) Open(r io.Reader) error {
	// set defaults if Init wasn't called
	if f.rdLen == 0 {
		f.FieldDelim = "\t"
		f.RecordDelim = "\n"
		f.rdLen = len([]byte(f.RecordDelim))
	}

	f.reader = r
	f.scanner = bufio.NewScanner(r)

	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			// blank last line
			return 0, nil, nil
		}
		datastr := string(data)
		if i := strings.Index(datastr, f.RecordDelim); i >= 0 {
			token = []byte(datastr[0:i])
			return len(token) + f.rdLen, token, nil
		}
		if atEOF {
			return len(data), data, nil
		}
		// request more data
		return 0, nil, nil
	}
	f.scanner.Split(split)
	return nil
}

func (f *SimpleDelimited) NextRecord() (string, error) {
	line := ""
	for line == "" {
		if !f.scanner.Scan() {
			return "", io.EOF
		}
		line = f.scanner.Text()
	}

	return line, nil
}

func (f *SimpleDelimited) GetFields(record string) (map[interface{}]string, error) {
	ret := make(map[interface{}]string)
	for i, v := range strings.Split(record, f.FieldDelim) {
		ret[i] = v
	}
	return ret, nil
}

func (f *SimpleDelimited) NextRecordFields() (map[interface{}]string, error) {
	s, e := f.NextRecord()
	if e != nil {
		return nil, e
	}
	return f.GetFields(s)
}

////////
////////
////////

// The CommaSeparated DataFormat describes an RFC 4180 format (as provided by encoding/csv). It
// only supports single-character delimiters for fields and comments.
type CommaSeparated struct {
	FieldDelim string
	Comment    string
	NumFields  int
	reader     io.Reader
	csvReader  *csv.Reader
}

// Init initializes the field and comment delimiters using spec["fields"] and spec["comments"],
// and the expected number of fields per record with spec["num_fields"].
func (f *CommaSeparated) Init(spec map[string]string) error {
	if v, found := spec["fields"]; found {
		if len(v) > 1 {
			return fmt.Errorf("Field delimiter for CommaSeparated format can only be one character long")
		}
		f.FieldDelim = v
	}
	if v, found := spec["comments"]; found {
		if len(v) > 1 {
			return fmt.Errorf("Comment delimiter for CommaSeparated format can only be one character long")
		}
		f.Comment = v
	}
	if v, found := spec["num_fields"]; found {
		_, err := fmt.Sscanf(v, "%d", &f.NumFields)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *CommaSeparated) Open(r io.Reader) error {
	f.reader = r
	f.csvReader = csv.NewReader(r)

	if f.FieldDelim != "" {
		f.csvReader.Comma, _ = utf8.DecodeRune([]byte(f.FieldDelim))
	}
	if f.Comment != "" {
		f.csvReader.Comment, _ = utf8.DecodeRune([]byte(f.Comment))
	}
	f.csvReader.FieldsPerRecord = f.NumFields

	return nil
}

// horribly inefficient, don't call this much!
func (f *CommaSeparated) NextRecord() (string, error) {
	rec, err := f.csvReader.Read()
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	if f.FieldDelim != "" {
		w.Comma, _ = utf8.DecodeRune([]byte(f.FieldDelim))
	}
	err = w.Write(rec)
	if err != nil {
		return "", err
	}
	w.Flush()

	return buf.String(), nil
}

// horribly inefficient, don't call this much!
func (f *CommaSeparated) GetFields(record string) (map[interface{}]string, error) {
	buf := bytes.NewBufferString(record)
	r := csv.NewReader(buf)
	if f.FieldDelim != "" {
		r.Comma, _ = utf8.DecodeRune([]byte(f.FieldDelim))
	}
	r.FieldsPerRecord = f.NumFields
	rec, err := r.Read()
	if err != nil {
		return nil, err
	}

	ret := make(map[interface{}]string)
	for i, v := range rec {
		ret[i] = v
	}
	return ret, nil
}

func (f *CommaSeparated) NextRecordFields() (map[interface{}]string, error) {
	rec, err := f.csvReader.Read()
	if err != nil {
		return nil, err
	}
	ret := make(map[interface{}]string)
	for i, v := range rec {
		ret[i] = v
	}
	return ret, nil
}

// The FixedWidth DataFormat describes a simple format where fields start at pre-defined
// character column boundaries and records are separated by newlines.
type FixedWidth struct {
	Offsets []int
	reader  io.Reader
	scanner *bufio.Scanner
}

// Init initializes the field column offsets using the comma-separated 0-index offsets
// found in spec["offsets"].
func (f *FixedWidth) Init(spec map[string]string) error {
	f.Offsets = nil

	if spec != nil {
		if offs, found := spec["offsets"]; found {
			for _, off := range strings.Split(offs, ",") {
				var n int
				_, err := fmt.Sscanf(off, "%d", &n)
				if err != nil {
					return err
				}
				f.Offsets = append(f.Offsets, n)
			}
		}
	}

	return nil
}

func (f *FixedWidth) Open(r io.Reader) error {
	f.reader = r
	f.scanner = bufio.NewScanner(r)

	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			// blank last line
			return 0, nil, nil
		}
		datastr := string(data)
		if i := strings.Index(datastr, "\n"); i >= 0 {
			token = []byte(datastr[0:i])
			return len(token) + len([]byte("\n")), token, nil
		}
		if atEOF {
			return len(data), data, nil
		}
		// request more data
		return 0, nil, nil
	}
	f.scanner.Split(split)
	return nil
}

func (f *FixedWidth) NextRecord() (string, error) {
	line := ""
	for line == "" {
		if !f.scanner.Scan() {
			return "", io.EOF
		}
		line = f.scanner.Text()
	}

	return line, nil
}

func (f *FixedWidth) GetFields(record string) (map[interface{}]string, error) {
	ret := make(map[interface{}]string)
	for i, v := range f.Offsets {
		if i == len(f.Offsets)-1 {
			ret[i] = record[v:]
		} else {
			ret[i] = record[v:f.Offsets[i+1]]
		}
	}
	return ret, nil
}

func (f *FixedWidth) NextRecordFields() (map[interface{}]string, error) {
	s, e := f.NextRecord()
	if e != nil {
		return nil, e
	}
	return f.GetFields(s)
}
