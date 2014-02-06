// Package filters provides a data-record filtering mechanism and basic implementations
// for typical use cases. It is intended as a complement to the formats sister package,
// useful for automating unique record extraction from a data file.
//
// A loose naming convention of adding "s" on the end implies that the filter is applied
// independently for each field of the record. Thus the missing "s" on "require" means that
// all supplied fields are required simultaneously. The currently supported filters are:
//
//    "require"      - drops any record that does NOT match ALL of it's field entries. An empty
//                     string ("") require field is skipped, so if you want to require records
//                     with blank fields, use the special string FilterBlankEntry
//
//    "excludes"     - drops any record matching at least one of it's field entries. An empty
//                     string ("") exclude field is skipped, so if you want to exclude records
//                     with blank fields, use the special string FilterBlankEntry
//
//                     To exclude multiple keywords from one field, you will either need to
//                     use multiple excludes or write a new Filter.
//
//    "null_fields"  - remaps fields from a placeholder string into an empty string. For
//                     example, many data sources use a placeholder of "-" or "n/a" to
//                     indicate a missing element. This filter may also be used to suppress
//                     particular values from records.
//
//    "split_fields" - splits fields on a delimiter, creating new records for each split. For
//                     example, a single record with 3="A,B,C" and a delimiter of "," emits
//                     three records with 3="A", 3="B" and 3="C".
//                     Note that the delimiter "" is not allowed.
//
//    "date_formats" - parses the field value using an strptime format string, and reformats
//                     it into a standard representation, of "2006-01-02 15:04:05" in UTC.
//                     Note that not all strptime formats are available, see the package
//                     at github.com/pbnjay/strptime for a listing.
//
// To support new filters, simply implement the Filter interface and call RegisterFilter before
// using GetFilter or FilterSet.Append.
//
package filters

import (
	"fmt"
	"strings"

	"github.com/pbnjay/strptime"
)

// Filter defines an interface that manipulates fields from one record into a new slice of
// records (most often 1-to-1). These manipulations can have optional parameters provided
// by Setup to control them.
type Filter interface {
	// Setup defines the part strings used to apply this filter to new records.
	Setup(parts map[interface{}]string) error
	// Apply takes an input record and applies the Filter to create 0 or more records.
	Apply(fields map[interface{}]string) []map[interface{}]string
}

type FilterGetter func() Filter

var (
	// FilterBlankEntry is a placeholder for blank string matching in RequireFilter and
	// ExcludeFilter. If for some reason your input contains this text and you need a
	// different representation, this may be overridden in user code.
	FilterBlankEntry = "<BLANK>"

	filters = make(map[string]FilterGetter)
)

///

type nullFilter struct {
	parts map[interface{}]string
}

func (f *nullFilter) Setup(parts map[interface{}]string) error {
	f.parts = parts
	return nil
}

func (f *nullFilter) Apply(fields map[interface{}]string) []map[interface{}]string {
	nnull := len(fields)
	for k, v := range f.parts {
		if v != "" {
			if v2, found := fields[k]; found && v2 == v {
				fields[k] = ""
				nnull--
			}
		}
	}
	if nnull == 0 {
		return nil
	}
	return []map[interface{}]string{fields}
}

///

type splitFieldFilter struct {
	parts map[interface{}]string
}

func (f *splitFieldFilter) Setup(parts map[interface{}]string) error {
	f.parts = parts
	return nil
}

func (f *splitFieldFilter) recApply(parts map[interface{}][]string, ki int, keys []interface{},
	lastmaps []map[interface{}]string) []map[interface{}]string {

	if len(keys) == ki {
		return lastmaps
	}

	k := keys[ki]
	if len(parts[k]) == 1 {
		// cover the easy case with one element
		for _, m := range lastmaps {
			m[k] = parts[k][0]
		}
		return f.recApply(parts, ki+1, keys, lastmaps)
	}

	newmaps := []map[interface{}]string{}
	for _, m := range lastmaps {
		for _, v := range parts[k] {
			m2 := make(map[interface{}]string)
			for k2, v2 := range m {
				m2[k2] = v2
			}
			m2[k] = v
			newmaps = append(newmaps, m2)
		}
	}
	return f.recApply(parts, ki+1, keys, newmaps)
}

func (f *splitFieldFilter) Apply(fields map[interface{}]string) []map[interface{}]string {
	allparts := make(map[interface{}][]string)
	keys := []interface{}{}

	// split all the fields
	for k, v := range f.parts {
		keys = append(keys, k)

		if v == "" {
			allparts[k] = []string{fields[k]}
		} else {
			if v2, found := fields[k]; found && strings.Contains(v2, v) {
				ss := []string{}
				for _, s := range strings.Split(v2, v) {
					if s != "" {
						ss = append(ss, s)
					}
				}
				allparts[k] = ss
			} else {
				allparts[k] = []string{fields[k]}
			}
		}
	}

	mstart := make(map[interface{}]string)
	return f.recApply(allparts, 0, keys, []map[interface{}]string{mstart})
}

///////

type requireFilter struct {
	parts map[interface{}]string
}

func (f *requireFilter) Setup(parts map[interface{}]string) error {
	f.parts = parts
	return nil
}

func (f *requireFilter) Apply(fields map[interface{}]string) []map[interface{}]string {
	for k, v := range f.parts {
		if v == "" {
			continue
		}

		if v == FilterBlankEntry {
			v = ""
		}
		if fields[k] != v {
			return nil
		}
	}
	return []map[interface{}]string{fields}
}

///////

type excludeFilter struct {
	parts map[interface{}]string
}

func (f *excludeFilter) Setup(parts map[interface{}]string) error {
	f.parts = parts
	return nil
}

func (f *excludeFilter) Apply(fields map[interface{}]string) []map[interface{}]string {
	for k, v := range f.parts {
		if v == "" {
			continue
		}

		if v == FilterBlankEntry {
			v = ""
		}
		if fields[k] == v {
			return nil
		}
	}
	return []map[interface{}]string{fields}
}

///////

type dateFormatFilter struct {
	parts map[interface{}]string
}

func (f *dateFormatFilter) Setup(parts map[interface{}]string) error {
	f.parts = parts

	// check date format strings are supported
	for _, v := range f.parts {
		if v == "" {
			continue
		}
		err := strptime.Check(v)
		if err != nil {
			return fmt.Errorf("error in date format filter '%s' - %s", v, err.Error())
		}
	}
	return nil
}

func (f *dateFormatFilter) Apply(fields map[interface{}]string) []map[interface{}]string {
	for k, v := range f.parts {
		if v == "" {
			continue
		}

		v2, found := fields[k]
		if !found || v2 == "" {
			continue
		}

		tm := strptime.MustParse(v2, v)
		fields[k] = tm.UTC().Format("2006-01-02 15:04:05")
	}
	return []map[interface{}]string{fields}
}

///////

// FilterSet defines an ordered set of filters that are applied to incoming data records. These
// filters can be use to restrict, reformat, and subdivide data into unique records. Filters
// are applied in the order they are added with Append(), so results are cumulative and early
// restrictions can bypass more expensive field splits.
type FilterSet struct {
	filters []Filter
}

// Append adds a new filter onto the end of the FilterSet chain.
func (fs *FilterSet) Append(ftype string, fields map[interface{}]string) error {
	fltr, err := GetFilter(ftype, fields)
	if err != nil {
		return err
	}

	fs.filters = append(fs.filters, fltr)
	return nil
}

// Apply calls Filter.Apply for each filter in the FilterSet, and accumulates results.
// Restrictive filters (such as Require/Exclude) should be applied as early as possible,
// and expansive filters (such as Split and DateFormat) should be applied as late as
// possible in order to decrease computational times.
func (fs *FilterSet) Apply(fields map[interface{}]string) []map[interface{}]string {
	lastset := []map[interface{}]string{fields}
	for _, fltr := range fs.filters {
		newset := []map[interface{}]string{}
		for _, mf := range lastset {
			for _, nf := range fltr.Apply(mf) {
				if len(nf) > 0 {
					newset = append(newset, nf)
				}
			}
		}
		// short-circuit nulls
		if len(newset) == 0 {
			return nil
		}
		lastset = newset
	}
	return lastset
}

///////

// RegisterFilter adds a new named Filter for discovery by GetFilter or FilterSet.Append.
func RegisterFilter(name string, fg FilterGetter) {
	filters[name] = fg
}

// GetFilter returns the named filter, initialized using Setup() with the fields parameter.
func GetFilter(name string, fields map[interface{}]string) (Filter, error) {
	fg, found := filters[name]

	if !found {
		return nil, fmt.Errorf("no registered filters match '%s'", name)
	}

	f := fg()

	err := f.Setup(fields)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func init() {
	RegisterFilter("null_fields", func() Filter { return &nullFilter{} })
	RegisterFilter("split_fields", func() Filter { return &splitFieldFilter{} })
	RegisterFilter("excludes", func() Filter { return &excludeFilter{} })
	RegisterFilter("require", func() Filter { return &requireFilter{} })
	RegisterFilter("date_formats", func() Filter { return &dateFormatFilter{} })
}
