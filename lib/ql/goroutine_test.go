package ql

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/stretchr/testify/require"
)

func TestReentrantWarn(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// put logger into context
	ctx := context.Background()
	ctx = sl.WithLogger(ctx, logger)

	func() {
		closeOne := reentrantWarn(ctx)
		defer closeOne()

		closeTwo := reentrantWarn(ctx)
		defer closeTwo()
	}()

	require.Contains(t, buf.String(), "Transaction in goroutine already exists")
}

func TestReentrantNoWarn(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// put logger into context
	ctx := context.Background()
	ctx = sl.WithLogger(ctx, logger)

	func() {
		closeOne := reentrantWarn(ctx)
		defer closeOne()
	}()

	require.NotContains(t, buf.String(), "Transaction in goroutine already exists")
}
