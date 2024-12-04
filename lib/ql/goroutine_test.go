package ql

import (
	"context"
	"testing"
)

func TestReentrantWarn(t *testing.T) {
	ctx := context.Background()

	closeOne := reentrantWarn(ctx)
	defer closeOne()

	closeTwo := reentrantWarn(ctx)
	defer closeTwo()
}
