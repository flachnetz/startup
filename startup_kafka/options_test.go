package startup_kafka_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/flachnetz/startup/v2"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_kafka"
	"github.com/stretchr/testify/require"
)

// parseKafkaOptions parses KafkaOptions from the given environment only, the way
// a service does it on startup.
func parseKafkaOptions(t *testing.T, env map[string]string) (startup_kafka.KafkaOptions, error) {
	t.Helper()

	old := os.Args
	os.Args = []string{"cmd"}
	t.Cleanup(func() { os.Args = old })

	t.Setenv("ENV_FILE", filepath.Join(t.TempDir(), "none.env"))
	t.Setenv("KAFKA_ADDRESS", "broker:9096")
	for key, value := range env {
		t.Setenv(key, value)
	}

	var opts struct {
		Base  startup_base.BaseOptions
		Kafka startup_kafka.KafkaOptions
	}
	opts.Base.ServiceName = "test"

	err := startup.ParseCommandLine(t.Context(), &opts)
	return opts.Kafka, err
}

func TestKafkaOptionsSASL(t *testing.T) {
	opts, err := parseKafkaOptions(t, map[string]string{
		"KAFKA_SECURITY_PROTOCOL": "sasl_ssl",
		"KAFKA_SASL_MECHANISM":    "SCRAM-SHA-512",
		"KAFKA_SASL_USERNAME":     "stardust",
		"KAFKA_SASL_PASSWORD":     "secret",
	})
	require.NoError(t, err)

	config := opts.DefaultConfig(nil)
	require.Equal(t, "sasl_ssl", config["security.protocol"])
	require.Equal(t, "SCRAM-SHA-512", config["sasl.mechanisms"])
	require.Equal(t, "stardust", config["sasl.username"])
	require.Equal(t, "secret", config["sasl.password"])
}

func TestKafkaOptionsSASLRequiresMechanism(t *testing.T) {
	_, err := parseKafkaOptions(t, map[string]string{"KAFKA_SECURITY_PROTOCOL": "sasl_ssl"})
	require.ErrorContains(t, err, "KafkaSaslMechanism")
}

func TestKafkaOptionsSSLHasNoSASLConfig(t *testing.T) {
	opts, err := parseKafkaOptions(t, map[string]string{"KAFKA_SASL_USERNAME": "ignored"})
	require.NoError(t, err)

	config := opts.DefaultConfig(nil)
	require.Equal(t, "ssl", config["security.protocol"])
	require.NotContains(t, config, "sasl.username")
}

func TestKafkaOptionsPropertiesFromEnv(t *testing.T) {
	pem := "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"

	opts, err := parseKafkaOptions(t, map[string]string{
		"KAFKA_PROPERTY": "ssl.ca.pem=" + pem + ";debug=broker,topic;",
	})
	require.NoError(t, err)

	config := opts.DefaultConfig(nil)
	require.Equal(t, pem, config["ssl.ca.pem"])
	require.Equal(t, "broker,topic", config["debug"])
}
