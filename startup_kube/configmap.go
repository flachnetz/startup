package startup_kube

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	sl "github.com/flachnetz/startup/v2/startup_logging"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/watch"
)

// ConfigMapValues contain strings and []byte values.
type ConfigMapValues map[string]any

// FetchConfigMapValues fetches the current state of a configmap
func FetchConfigMapValues(ctx context.Context, cs *kubernetes.Clientset, namespace, name string) (ConfigMapValues, error) {
	cm, err := cs.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read configmap %q.%q: %w", namespace, name, err)
	}

	return configmapToValues(cm), nil
}

type ConfigMapUpdate func(ConfigMapValues) error

// ObserveConfigMap observes a specific ConfigMap. If the ConfigMap already exists, the callback
// is invoked immediately. Otherwise, the callback is invoked when the ConfigMap is created.
// The callback is invoked with nil when the ConfigMap is deleted.
//
// ObserveConfigMap blocks until ctx is cancelled or an unrecoverable error occurs. When the
// underlying watch closes (for example on 410 Gone after an expired resourceVersion), it relists
// the ConfigMap, emits any missed change, and restarts the watcher.
func ObserveConfigMap(ctx context.Context, cs *kubernetes.Clientset, namespace, name string, callback ConfigMapUpdate) error {
	fieldSelector := fields.OneTermEqualSelector("metadata.name", name)
	listWatcher := cache.NewListWatchFromClient(cs.CoreV1().RESTClient(), "configmaps", namespace, fieldSelector)
	return observeConfigMap(ctx, listWatcher, namespace, name, time.Second, callback)
}

func observeConfigMap(
	ctx context.Context,
	listWatcher cache.ListerWatcherWithContext,
	namespace, name string,
	baseBackoff time.Duration,
	callback ConfigMapUpdate,
) error {
	log := sl.LoggerOf(ctx)

	var previousValue ConfigMapValues
	emitIfChanged := func(values ConfigMapValues) error {
		if reflect.DeepEqual(values, previousValue) {
			return nil
		}

		previousValue = values
		return callback(values)
	}

	backoff := baseBackoff
	maxBackoff := 30 * baseBackoff

	sleepBackoff := func() bool {
		t := time.NewTimer(backoff)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			backoff = min(backoff*2, maxBackoff)
			return true
		}
	}

	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil
		}

		obj, err := listWatcher.ListWithContext(ctx, metav1.ListOptions{})
		if err != nil {
			if attempt == 0 {
				return fmt.Errorf("initial list: %w", err)
			}
			if ctx.Err() != nil {
				return nil
			}
			log.WarnContext(ctx, "Failed to relist configmap, retrying",
				slog.String("namespace", namespace),
				slog.String("name", name),
				slog.Duration("backoff", backoff),
				sl.Error(err))
			if !sleepBackoff() {
				return nil
			}
			continue
		}

		list, ok := obj.(*v1.ConfigMapList)
		if !ok {
			return fmt.Errorf("unexpected list object type %T", obj)
		}

		var values ConfigMapValues
		if len(list.Items) > 0 {
			values = configmapToValues(&list.Items[0])
		}
		if err := emitIfChanged(values); err != nil {
			if attempt == 0 {
				return fmt.Errorf("initial callback: %w", err)
			}
			return fmt.Errorf("run callback: %w", err)
		}

		retryWatcher, err := watch.NewRetryWatcherWithContext(ctx, list.ResourceVersion, listWatcher)
		if err != nil {
			if attempt == 0 {
				return fmt.Errorf("create watcher: %w", err)
			}
			if ctx.Err() != nil {
				return nil
			}
			log.WarnContext(ctx, "Failed to restart configmap watcher, retrying",
				slog.String("namespace", namespace),
				slog.String("name", name),
				slog.Duration("backoff", backoff),
				sl.Error(err))
			if !sleepBackoff() {
				return nil
			}
			continue
		}

		watchStart := time.Now()
		sawEvent, err := consumeWatchEvents(ctx, log, retryWatcher, namespace, name, emitIfChanged)
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return nil
		}

		if sawEvent || time.Since(watchStart) >= maxBackoff {
			backoff = baseBackoff
		}

		log.WarnContext(ctx, "ConfigMap watcher closed, relisting",
			slog.String("namespace", namespace),
			slog.String("name", name),
			slog.Duration("backoff", backoff))

		if !sleepBackoff() {
			return nil
		}
	}
}

func consumeWatchEvents(
	ctx context.Context,
	log *slog.Logger,
	retryWatcher *watch.RetryWatcher,
	namespace, name string,
	emitIfChanged func(ConfigMapValues) error,
) (bool, error) {
	defer retryWatcher.Stop()

	sawEvent := false
	for event := range retryWatcher.ResultChan() {
		log.DebugContext(ctx, "Got event", slog.String("type", string(event.Type)))

		if event.Type == apiWatch.Error {
			log.WarnContext(ctx, "Ignoring error while observing configmap",
				slog.String("namespace", namespace),
				slog.String("name", name),
				slog.Any("object", event.Object))
			continue
		}

		cm, ok := event.Object.(*v1.ConfigMap)
		if !ok {
			continue
		}

		sawEvent = true
		var err error
		switch event.Type {
		case apiWatch.Added, apiWatch.Modified:
			err = emitIfChanged(configmapToValues(cm))
		case apiWatch.Deleted:
			err = emitIfChanged(nil)
		}

		if err != nil {
			return sawEvent, fmt.Errorf("run callback: %w", err)
		}
	}

	return sawEvent, nil
}

func WriteConfigMap(ctx context.Context, cs *kubernetes.Clientset, writer, namespace, name string, data ConfigMapValues) error {
	cm, err := valuesToConfigMap(name, namespace, data)
	if err != nil {
		return fmt.Errorf("serialize configmap: %w", err)
	}

	payload, err := json.Marshal(cm)
	if err != nil {
		return fmt.Errorf("serialize configmap to json: %w", err)
	}

	_, err = cs.CoreV1().ConfigMaps(namespace).Patch(
		ctx,
		name,
		types.ApplyPatchType,
		payload,
		metav1.PatchOptions{
			FieldManager: writer,
			Force:        new(true),
		},
	)

	return err
}

func valuesToConfigMap(name string, namespace string, data ConfigMapValues) (*v1.ConfigMap, error) {
	cm := &v1.ConfigMap{
		Kind:       "ConfigMap",
		APIVersion: "v1",
		Name:       name,
		Namespace:  namespace,
		Data:       make(map[string]string),
		BinaryData: make(map[string][]byte),
	}

	for key, value := range data {
		switch value := value.(type) {
		case string:
			cm.Data[key] = value

		case []byte:
			cm.BinaryData[key] = value

		default:
			return nil, fmt.Errorf("unsupported type: %T", value)
		}
	}

	return cm, nil
}

func configmapToValues(cm *v1.ConfigMap) ConfigMapValues {
	if cm == nil {
		return nil
	}

	result := make(ConfigMapValues, len(cm.Data)+len(cm.BinaryData))

	for key, value := range cm.Data {
		result[key] = value
	}

	for key, value := range cm.BinaryData {
		result[key] = value
	}
	return result
}
