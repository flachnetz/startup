package events

type NoopEventSender struct{}

func (s NoopEventSender) Init(event []Event) error {
	return nil
}

func (NoopEventSender) Send(event Event) {
}

func (NoopEventSender) SendBlocking(event Event) error {
	return nil
}

func (NoopEventSender) Close() error {
	return nil
}
