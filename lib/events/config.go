package events

import (
	"crypto/tls"
	confluent "github.com/Landoop/schema-registry"
	"github.com/Shopify/sarama"
	consul "github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/flachnetz/startup/v2/lib/schema"
)

type KafkaClientProvider interface {
	KafkaClient(addresses []string) (sarama.Client, error)
}

type Providers struct {
	Kafka  KafkaClientProvider
	Topics TopicsFunc
}

// Parses event sender config from string.
// an example could be
// --event-sender="confluent,address=http://confluent-registry.shared.svc.cluster.local,kafka=kafka.kafka.svc.cluster.local:9092,replication=1,blocking=true,schemainit=true"
// which uses confluent registry with kafka in blocking mode and initialises schemas at the registry during startup
//
// Sender Options:
//
// stdout: sends events to stdout
// noop: does not send anything at all
// stdout: sends events to stderr
// gzip,file=FILE: sends events to gziped filed
// kafka=URL: sends events to kafka
//
// Schema registries:
//
// consul,address=URL: uses consul as schema registry
// confluent,address=URL: uses confluent as schema registry
//
// Other options:
//
// replication=NUMBER: used to create the given kafka topics with the replication param
// blocking=true: will wait until the event got sent
func ParseEventSenders(providers Providers, config string) (EventSender, error) {
	reSenderType := regexp.MustCompile(`^([a-z]+)`)
	reArgument := regexp.MustCompile(`^,([a-zA-Z]+)=([^,]+)`)

	var eventSenders EventSenders

	for config != "" {
		match := reSenderType.FindStringSubmatch(config)
		if match == nil {
			return nil, errors.Errorf("expected event sender type at '%s'", shorten(config))
		}

		eventSenderType := match[1]
		config = config[len(match[0]):]

		argumentValues := map[string]string{}
		for len(config) > 0 && config[0] != ' ' {
			match := reArgument.FindStringSubmatch(config)
			if match == nil {
				return nil, errors.Errorf("expected argument at '%s'", shorten(config))
			}

			argumentValues[match[1]] = match[2]
			config = config[len(match[0]):]
		}

		eventSender, err := initializeEventSender(providers, eventSenderType, argumentValues)
		if err != nil {
			return nil, errors.WithMessage(err, "initializinig event sender")
		}

		eventSenders = append(eventSenders, eventSender)
	}

	return eventSenders, nil
}

func initializeEventSender(providers Providers, senderType string, arguments map[string]string) (EventSender, error) {
	switch senderType {
	case "noop":
		return NoopEventSender{}, nil

	case "stdout":
		return WriterEventSender{noopWriterCloser{os.Stdout}}, nil

	case "stderr":
		return WriterEventSender{noopWriterCloser{os.Stderr}}, nil

	case "gzip":
		if err := requireArguments(arguments, "file"); err != nil {
			return nil, errors.WithMessage(err, "gzip event sender")
		}

		return GZIPEventSender(arguments["file"])

	case "consul", "confluent":
		var encoder Encoder

		switch senderType {
		case "consul":
			if err := requireArguments(arguments, "kafka", "address"); err != nil {
				return nil, errors.WithMessage(err, "consul event sender")
			}

			consulClient, err := newConsulClient(arguments["address"])
			if err != nil {
				return nil, errors.Errorf("consul client")
			}

			encoder = NewAvroEncoder(schema.NewCachedRegistry(
				schema.NewConsulSchemaRegistry(consulClient)))

		case "confluent":
			if err := requireArguments(arguments, "kafka", "address"); err != nil {
				return nil, errors.WithMessage(err, "confluent event sender")
			}
			httpClient := &http.Client{
				Timeout: 3 * time.Second,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			}
			confluentClient, err := confluent.NewClient(arguments["address"], confluent.UsingClient(httpClient))
			if err != nil {
				return nil, errors.WithMessage(err, "confluent registry client")
			}

			encoder = NewAvroConfluentEncoder(confluentClient)
		}

		var err error

		replicationFactor := 1
		if value := arguments["replication"]; value != "" {
			replicationFactor, err = strconv.Atoi(value)
			if err != nil {
				return nil, errors.WithMessage(err, "replication factor")
			}
		}

		// split by spaces or commas
		kafkaAddresses := strings.FieldsFunc(arguments["kafka"], isCommaOrSpace)
		kafkaClient, err := providers.Kafka.KafkaClient(kafkaAddresses)
		if err != nil {
			return nil, errors.WithMessage(err, "create kafka client")
		}

		topics := providers.Topics(int16(replicationFactor))

		bufferSize := 1024
		if value := arguments["bufferSize"]; value != "" {
			bufferSize, err = strconv.Atoi(value)
			if err != nil {
				return nil, errors.WithMessage(err, "cannot parse buffer size")
			}
		}
		log.Infof("setting buffer size to %d", bufferSize)

		eventSender, err := NewKafkaSender(kafkaClient, KafkaSenderConfig{
			Encoder:         encoder,
			AllowBlocking:   arguments["blocking"] == "true",
			EventBufferSize: bufferSize,
			TopicsConfig:    topics,
		})

		if err != nil {
			return nil, errors.WithMessage(err, "kafka sender")
		}

		if len(topics.SchemaInitEvents) > 0 {
			var initEvents []Event
			for _, v := range topics.SchemaInitEvents {
				initEvents = append(initEvents, v)
			}
			err := eventSender.Init(initEvents)
			if err != nil {
				log.Errorf("event schema init failed")
				if topics.FailOnSchemaInit {
					return nil, errors.WithMessage(err, "event schema init failed")
				}
			}
		}

		return eventSender, nil
	}

	return nil, errors.Errorf("unknown event sender type: %s", senderType)
}

func requireArguments(arguments map[string]string, names ...string) error {
	for _, name := range names {
		if arguments[name] == "" {
			return errors.Errorf("missing argument '%s'", name)
		}
	}

	return nil
}

func shorten(str string) string {
	if len(str) > 16 {
		return str[:15] + "…"
	} else {
		return str
	}
}

type noopWriterCloser struct {
	io.Writer
}

func (noopWriterCloser) Close() error {
	return nil
}

func isCommaOrSpace(ch rune) bool {
	return ch == ',' || unicode.IsSpace(ch)
}

func newConsulClient(address string) (*consul.Client, error) {
	config := consul.DefaultConfig()
	config.Address = address

	return consul.NewClient(config)
}
