package formats

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"unicode"
)

type genericXMLFormat struct {
	descOffset int
	descent    []string
	records    map[string]bool
	reader     io.Reader
	decoder    *xml.Decoder
}

func (f *genericXMLFormat) Init(spec map[string]string) error {
	recs := strings.Split(spec["records"], ",")
	f.records = make(map[string]bool)
	for _, r := range recs {
		f.records[r] = true
	}
	return nil
}

func (f *genericXMLFormat) Open(r io.Reader) error {
	f.reader = r
	f.decoder = xml.NewDecoder(r)
	f.decoder.CharsetReader = charsetReader
	return nil
}

func (f *genericXMLFormat) xtractRecord() (map[string][]string, error) {
	var tok xml.Token
	var err error
	recData := make(map[string][]string)
	completeRecord := false
	parsingRecord := false

	// read until we get an expected record
	for tok, err = f.decoder.Token(); err == nil && !completeRecord; tok, err = f.decoder.Token() {
		switch tval := tok.(type) {
		case xml.StartElement:
			f.descent = append(f.descent, tval.Name.Local)
			if f.records[tval.Name.Local] {
				parsingRecord = true
				f.descOffset = len(f.descent) - 1
			}
		case xml.CharData:
			if strings.TrimSpace(string(tval)) == "" {
				continue
			}
			if parsingRecord {
				xPath := strings.Join(f.descent[f.descOffset:], ">")
				recData[xPath] = append(recData[xPath], string(tval))
			}
		case xml.EndElement:
			i := len(f.descent) - 1
			if parsingRecord && f.records[f.descent[i]] && len(recData) > 0 {
				completeRecord = true
			}
			if f.descent[i] == tval.Name.Local {
				f.descent = f.descent[:i]
			}
		}
	}
	return recData, err
}

func (f *genericXMLFormat) NextRecord() (string, error) {
	var rec map[string][]string
	var err error
	ret := []string{}
	for rec, err = f.xtractRecord(); err == nil && len(ret) == 0; rec, err = f.xtractRecord() {
		for key, val := range rec {
			ret = append(ret, key+" - "+strings.Join(val, "\t"))
		}
	}
	if err != nil {
		return "", err
	}
	return strings.Join(ret, "\n"), nil
}

func (f *genericXMLFormat) GetFields(record string) (map[interface{}]string, error) {
	ret := make(map[interface{}]string)
	for _, line := range strings.Split(record, "\n") {
		parts := strings.SplitN(line, " - ", 2)
		ret[parts[0]] = parts[1]
	}
	return ret, nil
}

func (f *genericXMLFormat) NextRecordFields() (map[interface{}]string, error) {
	var rec map[string][]string
	var err error
	ret := make(map[interface{}]string)
	for rec, err = f.xtractRecord(); err == nil && len(ret) == 0; rec, err = f.xtractRecord() {
		for key, val := range rec {
			ret[key] = strings.Join(val, "\t")
		}
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (f *genericXMLFormat) HasVariableFields() bool {
	return true
}

type charsetISO88591er struct {
	r   io.ByteReader
	buf *bytes.Buffer
}

func newCharsetISO88591(r io.Reader) *charsetISO88591er {
	buf := bytes.NewBuffer(make([]byte, 0, unicode.MaxRune))
	return &charsetISO88591er{r.(io.ByteReader), buf}
}

func (cs *charsetISO88591er) ReadByte() (b byte, err error) {
	// http://unicode.org/Public/MAPPINGS/ISO8859/8859-1.TXT
	// Date: 1999 July 27; Last modified: 27-Feb-2001 05:08
	if cs.buf.Len() <= 0 {
		r, err := cs.r.ReadByte()
		if err != nil {
			return 0, err
		}
		if r < unicode.MaxASCII {
			return r, nil
		}
		cs.buf.WriteRune(rune(r))
	}
	return cs.buf.ReadByte()
}

func (cs *charsetISO88591er) Read(p []byte) (int, error) {
	// Use ReadByte method.
	return 0, fmt.Errorf("not implemented")
}

func isCharset(charset string, names []string) bool {
	charset = strings.ToLower(charset)
	for _, n := range names {
		if charset == strings.ToLower(n) {
			return true
		}
	}
	return false
}

func isCharsetISO88591(charset string) bool {
	// http://www.iana.org/assignments/character-sets
	// (last updated 2010-11-04)
	names := []string{
		// Name
		"ISO_8859-1:1987",
		// Alias (preferred MIME name)
		"ISO-8859-1",
		// Aliases
		"iso-ir-100",
		"ISO_8859-1",
		"latin1",
		"l1",
		"IBM819",
		"CP819",
		"csISOLatin1",
	}
	return isCharset(charset, names)
}

func isCharsetUTF8(charset string) bool {
	names := []string{
		"UTF-8",
		// Default
		"",
	}
	return isCharset(charset, names)
}

func charsetReader(charset string, input io.Reader) (io.Reader, error) {
	if isCharsetUTF8(charset) {
		return input, nil
	}
	if isCharsetISO88591(charset) {
		return newCharsetISO88591(input), nil
	}
	return nil, fmt.Errorf("unexpected charset: " + charset)
}
