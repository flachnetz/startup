package events

import (
	"context"
	"crypto/tls"
	"database/sql"
	goerr "errors"
	confluent "github.com/Landoop/schema-registry"
	kafka2 "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var ErrNoAsyncSupport = goerr.New("event sender does not support async event sending")
var ErrNoTxSupport = goerr.New("event sender does not support transactional event sending")

func TimeToEventTimestamp(ts time.Time) int64 {
	return ts.UnixNano() / int64(time.Millisecond)
}

type KafkaSenderConfig struct {
	// Set to true to block Send() if the buffers are full.
	AllowBlocking bool

	// Topics configuration
	TopicsConfig EventTopics

	// The event encoder to use
	Encoder Encoder

	EventBufferSize int
}

var log = logrus.WithField("prefix", "events")

type Event interface {
	// Schema returns the avro schema of this event
	Schema() string

	// Serialize writes the event (in avro format) to the given writer.
	Serialize(io.Writer) error
}

type EventSender interface {
	// Close the event sender and flush all pending events.
	// Waits for all events to be send out.
	Close() error
}

type AsyncEventSender interface {
	EventSender

	// Send the given event. This method should be non blocking and
	// must never fail. You might want to use a channel for buffering
	// events internally. Errors will be logged to the terminal
	// but otherwise ignored.
	Send(event Event)
}

type TransactionalEventSender interface {
	EventSender

	// SendInTx Send Sends the message in the transaction.
	// Returns an error, if sending fails.
	SendInTx(ctx context.Context, tx *sql.Tx, event Event) error
}

// Events is the global instance to send events.
// Defaults to a simple sender that prints events using a logger instance.
var Events EventSender = LogrusEventSender{logrus.WithField("prefix", "events")}

// ParseEventSenders parses event sender config from string.
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
// confluent,address=URL: uses confluent as schema registry
//
// Other options:
//
// replication=NUMBER: used to create the given kafka topics with the replication param
// blocking=true: will wait until the event got sent
func ParseEventSenders(clientId string, topicsFunc TopicsFunc, config string, disableTls bool, configMap map[string]interface{}) (EventSender, error) {
	reSenderType := regexp.MustCompile(`^([a-z]+)`)
	reArgument := regexp.MustCompile(`^,([a-zA-Z]+)=([^,]+)`)

	if config == "" {
		return NoopEventSender{}, nil
	}

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

	eventSender, err := initializeEventSender(clientId, topicsFunc, eventSenderType, argumentValues, disableTls, configMap)
	if err != nil {
		return nil, errors.WithMessage(err, "initializing event sender")
	}

	return eventSender, nil
}

func initializeEventSender(clientId string, topicsFunc TopicsFunc, senderType string, arguments map[string]string, disableTls bool, configMap map[string]interface{}) (EventSender, error) {
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

	case "database":
		lookupTopic := topicForEventFunc(topicsFunc(1).TopicForType)
		return &PostgresEventSender{lookupTopic}, nil

	case "confluent":
		encoder, err := getEncoder(arguments)
		if err != nil {
			return nil, errors.WithMessage(err, "create encoder")
		}

		replicationFactor := 1
		if value := arguments["replication"]; value != "" {
			replicationFactor, err = strconv.Atoi(value)
			if err != nil {
				return nil, errors.WithMessage(err, "replication factor")
			}
		}

		// split by spaces or commas
		kafkaAddresses := strings.FieldsFunc(arguments["kafka"], isCommaOrSpace)
		producerConfigMap := kafka2.ConfigMap{
			"client.id":         clientId,
			"bootstrap.servers": strings.Join(kafkaAddresses, ","),
			"partitioner":       "murmur2_random",
		}
		if !disableTls {
			producerConfigMap["security.protocol"] = "ssl"
		}
		for k, v := range configMap {
			producerConfigMap[k] = v
		}

		producer, err := kafka2.NewProducer(&producerConfigMap)
		if err != nil {
			return nil, errors.WithMessage(err, "kafka producer")
		}

		go func() {
			logger := startup_logrus.GetLogger(context.Background(), "kafka-delivery-channel")
			for e := range producer.Events() {
				switch ev := e.(type) {
				case *kafka2.Message:
					if ev.TopicPartition.Error != nil {
						logger.Errorf("Failed to deliver message: %v", ev.TopicPartition)
					} else {
						logger.Debugf("Successfully produced record to topic %s partition [%d] @ offset %v",
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			}
		}()

		topics := topicsFunc(int16(replicationFactor))

		bufferSize := 1024
		if value := arguments["bufferSize"]; value != "" {
			bufferSize, err = strconv.Atoi(value)
			if err != nil {
				return nil, errors.WithMessage(err, "cannot parse buffer size")
			}
		}
		log.Infof("setting buffer size to %d", bufferSize)

		senderConfig := KafkaSenderConfig{
			Encoder:         encoder,
			AllowBlocking:   arguments["blocking"] == "true",
			EventBufferSize: bufferSize,
			TopicsConfig:    topics,
		}

		eventSender, err := NewKafkaConfluentSender(producer, senderConfig)
		if err != nil {
			return nil, errors.WithMessage(err, "kafka sender")
		}

		if err := initializeKafkaSender(eventSender, topics); err != nil {
			return nil, err
		}

		return eventSender, nil
	}

	return nil, errors.Errorf("unknown event sender type: %s", senderType)
}

func initializeKafkaSender(eventSender *KafkaConfluentSender, topics EventTopics) error {
	if len(topics.SchemaInitEvents) == 0 {
		return nil
	}

	var initEvents []Event
	for _, v := range topics.SchemaInitEvents {
		initEvents = append(initEvents, v)
	}

	if err := eventSender.Init(initEvents); err != nil {
		log.Errorf("event schema init failed: %s", err.Error())
		if topics.FailOnSchemaInit {
			return errors.WithMessage(err, "event schema init failed")
		}
	}

	return nil
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

func getEncoder(arguments map[string]string) (Encoder, error) {
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

	return NewAvroConfluentEncoder(confluentClient), nil
}
