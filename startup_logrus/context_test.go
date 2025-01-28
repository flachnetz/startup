package startup_logrus

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestGetLogger(t *testing.T) {
	ctx := context.Background()

	log := logrus.WithField("a", 1)
	ctx = WithLogger(ctx, log)

	value := GetLogger(ctx).Data[0].Value.Int64()
	if value != 1 {
		t.Fatalf("value should be 1, but was %d", value)
	}
}

func TestPrefixOf(t *testing.T) {
	if prefix := prefixOf("test"); prefix != "test" {
		t.Fatalf("expected prefix 'test' but got '%s'", prefix)
	}

	if prefix := prefixOf(nil); prefix != "" {
		t.Fatalf("expected prefix '' but got '%s'", prefix)
	}

	if prefix := prefixOf(someStringer{}); prefix != "From Stringer" {
		t.Fatalf("expected prefix 'From Stringer' but got '%s'", prefix)
	}

	if prefix := prefixOf(myService{}); prefix != "myService" {
		t.Fatalf("expected prefix 'myService' but got '%s'", prefix)
	}

	if prefix := prefixOf(&someStringer{}); prefix != "From Stringer" {
		t.Fatalf("expected prefix 'From Stringer' but got '%s'", prefix)
	}

	if prefix := prefixOf(&myService{}); prefix != "myService" {
		t.Fatalf("expected prefix 'myService' but got '%s'", prefix)
	}

	if prefix := prefixOf(TestPrefixOf); prefix != "TestPrefixOf" {
		t.Fatalf("expected prefix 'TestPrefixOf' but got '%s'", prefix)
	}

	innerFunction := func() {}

	if prefix := prefixOf(innerFunction); prefix != "TestPrefixOf" {
		t.Fatalf("expected prefix 'TestPrefixOf' but got '%s'", prefix)
	}
}

type someStringer struct{}

func (someStringer) String() string {
	return "From Stringer"
}

type myService struct{}
