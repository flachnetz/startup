package kafka

type Topics []Topic

type TopicsFunc func(replicationFactor int16) Topics


type Topic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Config            map[string]*string
}
