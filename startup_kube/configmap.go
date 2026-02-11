package startup_kube

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/flachnetz/startup/v2/lib"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
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
	listOptions := metav1.ListOptions{
		FieldSelector: "metadata.name=" + name,
	}

	watcher, err := cs.CoreV1().ConfigMaps(namespace).Watch(ctx, listOptions)
	if err != nil {
		return fmt.Errorf("watch configmap: %w", err)
	}

	defer watcher.Stop()

	var previousValue ConfigMapValues

	emitIfChanged := func(values ConfigMapValues) error {
		if maps.Equal(values, previousValue) {
			return nil
		}

		previousValue = values
		return callback(values)
	}

	for event := range watcher.ResultChan() {
		cm, ok := event.Object.(*v1.ConfigMap)
		if !ok {
			continue
		}

		switch event.Type {
		case watch.Added, watch.Modified:
			err = emitIfChanged(configmapToValues(cm))
		case watch.Deleted:
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
		return fmt.Errorf("serialize configmap: %s", err)
	}

	payload, err := json.Marshal(cm)
	if err != nil {
		return fmt.Errorf("serialize configmap to json: %s", err)
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

	result := make(ConfigMapValues)

	for key, value := range cm.Data {
		result[key] = value
	}

	for key, value := range cm.BinaryData {
		result[key] = value
	}
	return result
}
