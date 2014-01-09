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

type simpleDelimited struct {
	FieldDelim  string
	RecordDelim string
	rdLen       int
	reader      io.Reader
	scanner     *bufio.Scanner
}

func (f *simpleDelimited) Init(spec map[string]string) error {
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

func (f *simpleDelimited) Open(r io.Reader) error {
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

func (f *simpleDelimited) NextRecord() (string, error) {
	line := ""
	for line == "" {
		if !f.scanner.Scan() {
			return "", io.EOF
		}
		line = f.scanner.Text()
	}

	return line, nil
}

func (f *simpleDelimited) GetFields(record string) (map[interface{}]string, error) {
	ret := make(map[interface{}]string)
	for i, v := range strings.Split(record, f.FieldDelim) {
		ret[i] = v
	}
	return ret, nil
}

func (f *simpleDelimited) NextRecordFields() (map[interface{}]string, error) {
	s, e := f.NextRecord()
	if e != nil {
		return nil, e
	}
	return f.GetFields(s)
}

////////
////////
////////

type commaSeparated struct {
	FieldDelim string
	Comment    string
	NumFields  int
	reader     io.Reader
	csvReader  *csv.Reader
}

func (f *commaSeparated) Init(spec map[string]string) error {
	if v, found := spec["fields"]; found {
		if len(v) > 1 {
			return fmt.Errorf("field delimiter for csv format can only be one character long")
		}
		f.FieldDelim = v
	}
	if v, found := spec["comments"]; found {
		if len(v) > 1 {
			return fmt.Errorf("comment delimiter for csv format can only be one character long")
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

func (f *commaSeparated) Open(r io.Reader) error {
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
func (f *commaSeparated) NextRecord() (string, error) {
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
func (f *commaSeparated) GetFields(record string) (map[interface{}]string, error) {
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

func (f *commaSeparated) NextRecordFields() (map[interface{}]string, error) {
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

/////////

type fixedWidth struct {
	Offsets []int
	reader  io.Reader
	scanner *bufio.Scanner
}

func (f *fixedWidth) Init(spec map[string]string) error {
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

func (f *fixedWidth) Open(r io.Reader) error {
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

func (f *fixedWidth) NextRecord() (string, error) {
	line := ""
	for line == "" {
		if !f.scanner.Scan() {
			return "", io.EOF
		}
		line = f.scanner.Text()
	}

	return line, nil
}

func (f *fixedWidth) GetFields(record string) (map[interface{}]string, error) {
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

func (f *fixedWidth) NextRecordFields() (map[interface{}]string, error) {
	s, e := f.NextRecord()
	if e != nil {
		return nil, e
	}
	return f.GetFields(s)
}
