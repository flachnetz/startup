package goid

import (
	"reflect"
	"unsafe"
)

// iface The interface struct.
type iface struct {
	tab  unsafe.Pointer
	data unsafe.Pointer
}

// typelinks returns a slice of the sections in each module,
// and a slice of *rtype offsets in each module.
// The types in each module are sorted by string.
//
//go:linkname typelinks reflect.typelinks
func typelinks() (sections []unsafe.Pointer, offset [][]int32)

// resolveTypeOff resolves an *rtype offset from a base type.
//
//go:linkname resolveTypeOff reflect.resolveTypeOff
func resolveTypeOff(rtype unsafe.Pointer, off int32) unsafe.Pointer

// typeCursor is one reflect.Type value that gets re-aimed at arbitrary *rtype
// offsets instead of allocating a type per candidate. The cursor holds a pointer
// into its own typ field, so it must not be copied - always pass it around as
// *typeCursor.
type typeCursor struct {
	typ  reflect.Type
	face *iface
}

// newTypeCursor returns a cursor initially pointing at *int.
func newTypeCursor() *typeCursor {
	// typ is a struct iface{tab(ptr->reflect.Type), data(ptr->rtype)}
	c := &typeCursor{typ: reflect.TypeFor[int]()}
	c.face = (*iface)(unsafe.Pointer(&c.typ)) // #nosec G103 -- audited: deliberate iface layout access

	return c
}

// at aims the cursor at the type stored at off within section.
func (c *typeCursor) at(section unsafe.Pointer, off int32) reflect.Type {
	c.face.data = resolveTypeOff(section, off)

	return c.typ
}

// typeByString returns the type whose 'String' property equals to the given string,
// or nil if not found.
//
//go:linkname typeByString routine.typeByString
func typeByString(str string) reflect.Type {
	// the search target is always the pointer form, because that is what the
	// type sections carry
	s := str
	if len(str) == 0 || str[0] != '*' {
		s = "*" + s
	}

	cursor := newTypeCursor()

	sections, offset := typelinks()
	for offsI, offs := range offset {
		section := sections[offsI]

		i := searchTypeOffset(cursor, section, offs, s)
		if i >= len(offs) {
			continue
		}

		if typ := matchingType(cursor.at(section, offs[i]), str); typ != nil {
			return typ
		}
	}

	return nil
}

// searchTypeOffset returns the first index in offs whose type string sorts at or
// after s. This is a copy of sort.Search, with f(h) replaced by
// (*typ[h].String() >= s), because the candidate type has to be resolved for
// every probe.
func searchTypeOffset(cursor *typeCursor, section unsafe.Pointer, offs []int32, s string) int {
	i, j := 0, len(offs)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i <= h < j
		if cursor.at(section, offs[h]).String() < s {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}

	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i
}

// matchingType reports which of typ or its element has exactly the name str, or
// nil when neither does. Only pointer types can match, since the sections are
// searched in pointer form.
func matchingType(typ reflect.Type, str string) reflect.Type {
	if typ.Kind() != reflect.Pointer {
		return nil
	}

	if typ.String() == str {
		return typ
	}

	if elem := typ.Elem(); elem.String() == str {
		return elem
	}

	return nil
}
