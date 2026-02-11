package main

import (
	"context"
	"fmt"
	"time"

	"github.com/flachnetz/startup/v2"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_kube"
)

func main() {
	var opts struct {
		startup_kube.KubernetesOptions
	}

	startup.MustParseCommandLine(&opts)

	client := opts.Client()

	go func() {
		err := startup_kube.ObserveConfigMap(context.TODO(), client, "zig", "test", func(cm startup_kube.ConfigMapValues) error {
			fmt.Println(cm)
			return nil
		})

		startup_base.FatalOnError(err, "Observe configmap")
	}()

	time.Sleep(2 * time.Second)

	fmt.Println("Write configmap now")
	err := startup_kube.WriteConfigMap(context.TODO(), client, "startup-lib", "zig", "test", map[string]any{
		"foo": "bar4",
	})

	startup_base.FatalOnError(err, "Write configmap")

	time.Sleep(100 * time.Second)
}
