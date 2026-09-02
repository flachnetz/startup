package goid

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTypeByString pins the behaviour typeByString is used for: resolving a
// runtime type by name, both for a type from this package and for the unexported
// runtime type goid actually needs.
func TestTypeByString(t *testing.T) {
	cases := []struct {
		name string
		want reflect.Type
	}{
		{name: "runtime.g"},
		{name: "goid.iface", want: reflect.TypeFor[iface]()},
		{name: "no.such.type.exists"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := typeByString(c.name)

			switch {
			case c.name == "runtime.g":
				// the type is unexported, so it can only be checked by name
				require.NotNil(t, got)
				require.Equal(t, "runtime.g", got.String())

			case c.want == nil:
				require.Nil(t, got)

			default:
				require.Equal(t, c.want, got)
			}
		})
	}
}

// TestTypeByStringPointerForm covers the "*T" input form, which the search
// normalises internally.
func TestTypeByStringPointerForm(t *testing.T) {
	got := typeByString("*goid.iface")
	require.NotNil(t, got)
	require.Equal(t, reflect.TypeFor[*iface](), got)
}
