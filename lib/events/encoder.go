package events

type Encoder interface {
	// Encodes an event into some kind of binary representation.
	Encode(event Event) ([]byte, error)

	// Close the encoder.
	Close() error
}
