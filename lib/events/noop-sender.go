package events

type NoopEventSender struct{}

func (s NoopEventSender) Init([]Event) error {
	return nil
}

func (NoopEventSender) Send(Event) {
}

func (NoopEventSender) SendBlocking(Event) error {
	return nil
}

func (NoopEventSender) Close() error {
	return nil
}
