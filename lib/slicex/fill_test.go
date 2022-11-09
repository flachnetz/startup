package slicex

import (
	"reflect"
	"testing"
)

type testStruct struct {
	name   string
	number int
}

func TestFill(t *testing.T) {
	type args struct {
		value  testStruct
		number int
	}
	tests := []struct {
		name string
		args args
		want []testStruct
	}{
		{
			name: "testStruct 5",
			args: args{
				value: testStruct{
					name:   "name",
					number: 77,
				},
				number: 5,
			},
			want: []testStruct{{name: "name", number: 77}, {name: "name", number: 77}, {name: "name", number: 77}, {name: "name", number: 77}, {name: "name", number: 77}},
		},
		{
			name: "testStruct empty",
			args: args{
				value:  testStruct{},
				number: 0,
			},
			want: []testStruct{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Fill(tt.args.value, tt.args.number); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Fill() = %v, want %v", got, tt.want)
			}
		})
	}
}
