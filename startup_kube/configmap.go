package startup_kube

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/flachnetz/startup/v2/lib"
	"github.com/flachnetz/startup/v2/startup_logrus"
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
// ObserveConfigMap will block forever, or until an error occurs.
func ObserveConfigMap(ctx context.Context, cs *kubernetes.Clientset, namespace, name string, callback ConfigMapUpdate) error {
	log := startup_logrus.LoggerOf(ctx)

	var previousValue ConfigMapValues

	emitIfChanged := func(values ConfigMapValues) error {
		if reflect.DeepEqual(values, previousValue) {
			return nil
		}

		previousValue = values
		return callback(values)
	}

	fieldSelector := fields.OneTermEqualSelector("metadata.name", name)

	// get initial version of the resource list so we can use it as a start when watching
	// for updates
	list, err := cs.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector.String(),
	})
	if err != nil {
		return fmt.Errorf("initial list: %w", err)
	}

	if len(list.Items) > 0 {
		// emit initial state
		err := emitIfChanged(configmapToValues(&list.Items[0]))
		if err != nil {
			return fmt.Errorf("initial callback: %w", err)
		}
	}

	// create a new watcher for lists of configmaps filtered by name
	listWatcher := cache.NewListWatchFromClient(cs.CoreV1().RESTClient(), "configmaps", namespace, fieldSelector)
	retryWatcher, err := watch.NewRetryWatcherWithContext(ctx, list.ResourceVersion, listWatcher)
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}

	defer retryWatcher.Stop()

	for event := range retryWatcher.ResultChan() {
		log.Debugf("Got event: %q", event.Type)

		if event.Type == apiWatch.Error {
			log.Warnf("Ignoring error while observing %q.%q: %s", namespace, name, event.Object)
			continue
		}

		cm, ok := event.Object.(*v1.ConfigMap)
		if !ok {
			continue
		}

		switch event.Type {
		case apiWatch.Added, apiWatch.Modified:
			err = emitIfChanged(configmapToValues(cm))
		case apiWatch.Deleted:
			err = emitIfChanged(nil)
		}

		if err != nil {
			return fmt.Errorf("run callback: %w", err)
		}
	}

	return nil
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
			// valid in go 1.26
			// Force:        new(true)

			Force: lib.PtrOf(true),
		},
	)

	return err
}

func valuesToConfigMap(name string, namespace string, data ConfigMapValues) (*v1.ConfigMap, error) {
	cm := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
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
