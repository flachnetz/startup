package timejump

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	clock2 "github.com/benbjohnson/clock"
	"github.com/flachnetz/startup/v2/lib/clock"
	"github.com/flachnetz/startup/v2/startup_base"
	kube "github.com/flachnetz/startup/v2/startup_kube"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Options struct {
	Enabled      bool   `long:"timejump-enabled" env:"TIMEJUMP_ENABLED" description:"Set true if you want timejumps to be nabled"`
	Namespace    string `long:"timejump-namespace" env:"TIMEJUMP_NAMESPACE" default:"default" description:"Namespace to look for the timejump config, defaults to default"`
	ResourceName string `long:"timejump-resource-name" env:"TIMEJUMP_RESOURCE_NAME" default:"timejump" description:"Name of the configmap that contains timejump configuration"`
}

func (o *Options) Initialize(ctx context.Context, kubeOpts kube.KubernetesOptions) {
	if !o.Enabled {
		return
	}

	kubeClient := kubeOpts.Client()

	slog.InfoContext(ctx, "Fetch current time-jump offset")

	values, err := o.fetchInitial(ctx, kubeClient)
	startup_base.FatalOnError(err, "Read ConfigMap for timejump")

	// initialize the time offset
	var timeOffset atomic.Pointer[time.Duration]
	timeOffset.Store(new(0 * time.Second))
	applyConfigMapValues(ctx, values, &timeOffset)

	// create mock clock
	clock.GlobalClock = createMockClock(&timeOffset)

	go func() {
		err := kube.ObserveConfigMap(ctx, kubeClient, o.Namespace, o.ResourceName, func(values kube.ConfigMapValues) error {
			applyConfigMapValues(ctx, values, &timeOffset)
			return nil
		})

		startup_base.FatalOnError(err, "Observe time offset")
	}()
}

func (o *Options) fetchInitial(ctx context.Context, kubeClient *kubernetes.Clientset) (kube.ConfigMapValues, error) {
	values, err := kube.FetchConfigMapValues(ctx, kubeClient, o.Namespace, o.ResourceName)

	if err, ok := errors.AsType[*errors2.StatusError](err); ok {
		if err.ErrStatus.Reason == v1.StatusReasonNotFound {
			// this is fine, it might just not exist right now
			return nil, nil
		}

		return nil, fmt.Errorf("fetch configmap for timejump: %w", err)
	}

	return values, nil
}

func applyConfigMapValues(ctx context.Context, values kube.ConfigMapValues, timeOffset *atomic.Pointer[time.Duration]) {
	previousOffset := *timeOffset.Load()

	offset, ok := values["offsetInSeconds"]
	if !ok {
		return
	}

	offsetStr, ok := offset.(string)
	if !ok {
		slog.WarnContext(ctx, "offsetInSeconds is not a string")
		return
	}

	offsetVal, err := strconv.Atoi(offsetStr)
	if err != nil {
		slog.WarnContext(ctx, "offsetInSeconds cannot be parsed", slog.String("value", offsetStr), sl.Error(err))
		return
	}

	off := time.Duration(offsetVal) * time.Second
	if off < previousOffset {
		slog.WarnContext(ctx, "offsetInSeconds cannot be less than previous value",
			slog.Duration("prevValue", *timeOffset.Load()),
			slog.Duration("newValue", off))
		return
	}

	if off == previousOffset {
		// no change
		return
	}

	slog.InfoContext(ctx, "Got new offset", slog.Duration("offset", off))

	timeOffset.Store(new(off))
}

func createMockClock(offset *atomic.Pointer[time.Duration]) clock.Clock {
	mockClock := clock2.NewMock()
	mockClock.Set(time.Now())

	go func() {
		var previousOffset time.Duration

		for {
			offset := *offset.Load()
			newTime := time.Now().Add(offset)

			if previousOffset != offset {
				slog.Info("Performing time-jump now",
					slog.Duration("jump", offset-previousOffset),
					slog.Time("newTime", newTime))

				previousOffset = offset
			}

			// calculate the new current time
			mockClock.Set(newTime)

			time.Sleep(25 * time.Millisecond)
		}
	}()

	return mockClock
}
