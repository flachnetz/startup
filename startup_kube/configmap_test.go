package startup_kube

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiWatch "k8s.io/apimachinery/pkg/watch"
)

type fakeListWatcher struct {
	mu      sync.Mutex
	lists   []func() (*v1.ConfigMapList, error)
	watches []func(rv string) (apiWatch.Interface, error)
	watchRV []string
}

func (f *fakeListWatcher) ListWithContext(_ context.Context, _ metav1.ListOptions) (runtime.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.lists) == 0 {
		return nil, errors.New("unexpected List call")
	}
	fn := f.lists[0]
	f.lists = f.lists[1:]
	return fn()
}

func (f *fakeListWatcher) WatchWithContext(_ context.Context, opts metav1.ListOptions) (apiWatch.Interface, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.watchRV = append(f.watchRV, opts.ResourceVersion)
	if len(f.watches) == 0 {
		return nil, errors.New("unexpected Watch call")
	}
	fn := f.watches[0]
	f.watches = f.watches[1:]
	return fn(opts.ResourceVersion)
}

func configMapList(rv string, items ...v1.ConfigMap) *v1.ConfigMapList {
	return &v1.ConfigMapList{
		ListMeta: metav1.ListMeta{ResourceVersion: rv},
		Items:    items,
	}
}

func configMap(rv, offset string) v1.ConfigMap {
	return v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "timejump",
			Namespace:       "default",
			ResourceVersion: rv,
		},
		Data: map[string]string{
			"offsetInSeconds": offset,
		},
	}
}

func goneStatus() *metav1.Status {
	return &metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonExpired,
		Code:    http.StatusGone,
		Message: "too old resource version",
	}
}

func TestObserveConfigMapRecoversFrom410Gone(t *testing.T) {
	w1 := apiWatch.NewFake()
	w2 := apiWatch.NewFake()

	lw := &fakeListWatcher{
		lists: []func() (*v1.ConfigMapList, error){
			func() (*v1.ConfigMapList, error) {
				return configMapList("10", configMap("10", "100")), nil
			},
			// Transient relist failure after 410 Gone must be retried.
			func() (*v1.ConfigMapList, error) {
				return nil, errors.New("apiserver temporarily unavailable")
			},
			// State changed while disconnected from 100 to 200.
			func() (*v1.ConfigMapList, error) {
				return configMapList("20", configMap("20", "200")), nil
			},
		},
		watches: []func(string) (apiWatch.Interface, error){
			func(string) (apiWatch.Interface, error) { return w1, nil },
			func(string) (apiWatch.Interface, error) { return w2, nil },
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := make(chan ConfigMapValues, 10)
	done := make(chan error, 1)
	go func() {
		done <- observeConfigMap(ctx, lw, "default", "timejump", 5*time.Millisecond, func(v ConfigMapValues) error {
			updates <- v
			return nil
		})
	}()

	waitUpdate := func(want ConfigMapValues) {
		t.Helper()
		select {
		case got := <-updates:
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got update %v, want %v", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for update %v", want)
		}
	}

	// 1. Initial list emits 100.
	waitUpdate(ConfigMapValues{"offsetInSeconds": "100"})

	// 2. First watch hits 410 Gone; RetryWatcher closes its ResultChan.
	w1.Error(goneStatus())

	// 3. observeConfigMap retries the transient list failure, relists, and emits 200.
	waitUpdate(ConfigMapValues{"offsetInSeconds": "200"})

	// 4. Second watch receives a live modification and emits 300.
	cm3 := configMap("21", "300")
	w2.Modify(&cm3)
	waitUpdate(ConfigMapValues{"offsetInSeconds": "300"})

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("observeConfigMap returned error on cancel: %v", err)
	}

	lw.mu.Lock()
	defer lw.mu.Unlock()
	wantRVs := []string{"10", "20"}
	if !reflect.DeepEqual(lw.watchRV, wantRVs) {
		t.Fatalf("watch resource versions = %v, want %v", lw.watchRV, wantRVs)
	}
}

func TestObserveConfigMapEmitsDeletionMissedDuring410Gone(t *testing.T) {
	w1 := apiWatch.NewFake()
	w2 := apiWatch.NewFake()

	lw := &fakeListWatcher{
		lists: []func() (*v1.ConfigMapList, error){
			func() (*v1.ConfigMapList, error) {
				return configMapList("10", configMap("10", "100")), nil
			},
			// ConfigMap was deleted while disconnected.
			func() (*v1.ConfigMapList, error) {
				return configMapList("15"), nil
			},
		},
		watches: []func(string) (apiWatch.Interface, error){
			func(string) (apiWatch.Interface, error) { return w1, nil },
			func(string) (apiWatch.Interface, error) { return w2, nil },
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := make(chan ConfigMapValues, 10)
	done := make(chan error, 1)
	go func() {
		done <- observeConfigMap(ctx, lw, "default", "timejump", 5*time.Millisecond, func(v ConfigMapValues) error {
			updates <- v
			return nil
		})
	}()

	select {
	case got := <-updates:
		if !reflect.DeepEqual(got, ConfigMapValues{"offsetInSeconds": "100"}) {
			t.Fatalf("initial update = %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial update")
	}

	w1.Error(goneStatus())

	select {
	case got := <-updates:
		if got != nil {
			t.Fatalf("deletion update = %v, want nil", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deletion update")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("observeConfigMap returned error on cancel: %v", err)
	}
}

func TestObserveConfigMapReturnsInitialListError(t *testing.T) {
	wantErr := errors.New("forbidden")
	lw := &fakeListWatcher{
		lists: []func() (*v1.ConfigMapList, error){
			func() (*v1.ConfigMapList, error) { return nil, wantErr },
		},
	}

	err := observeConfigMap(context.Background(), lw, "default", "timejump", 5*time.Millisecond, func(ConfigMapValues) error {
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}
