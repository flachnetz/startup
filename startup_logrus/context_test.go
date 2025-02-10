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

	value := LoggerOf(ctx).Data[0].Value.Int64()
	if value != 1 {
		t.Fatalf("value should be 1, but was %d", value)
	}
}
