package events

import (
	"context"
	"sync"

	"github.com/jmoiron/sqlx"
)

type NoopEventSender struct {
	eventChannel     chan Event
	eventchannelInit sync.Once
}

func (n *NoopEventSender) SendAsync(ctx context.Context, event Event) {
}

func (n *NoopEventSender) SendInTx(ctx context.Context, tx sqlx.ExecerContext, event Event) error {
	return nil
}

func (n *NoopEventSender) Close() error {
	return nil
}

func (n *NoopEventSender) SendAsyncCh() chan<- Event {
	n.eventchannelInit.Do(func() {
		n.eventChannel = make(chan Event)

		go func() {
			for event := range n.eventChannel {
				n.SendAsync(context.Background(), event)
			}
		}()
	})

	return n.eventChannel
}
