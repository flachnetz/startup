package ulid

import (
	crand "crypto/rand"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/flachnetz/startup/v2/lib/clock"
	"github.com/google/uuid"
	"github.com/oklog/ulid"
)

func Generate() ULID {
	monotonicLock.Lock()
	defer monotonicLock.Unlock()
	token := ulid.MustNew(ulid.Timestamp(clock.GlobalClock.Now()), monotonic)

	return ULID(token)
}

// ULID is an extended ulid implementation.
//
// A ULID serializes to JSON and string as the standard 26 char ulid text form,
// but it can be deserialized from a UUID. This is helpful when storing ULIDs as
// UUID in a postgres database.
type ULID [16]byte

func (u ULID) String() string {
	return ulid.ULID(u).String()
}

func (u ULID) LogValue() slog.Value {
	return slog.StringValue(u.String())
}

func (u ULID) MarshalText() (text []byte, err error) {
	return []byte(u.String()), nil
}

func (u ULID) Timestamp() time.Time {
	return time.UnixMilli(int64(ulid.ULID(u).Time()))
}

//goland:noinspection GoMixedReceiverTypes
func (u *ULID) UnmarshalText(text []byte) error {
	src := string(text)

	switch len(src) {
	case 26:
		// encoded as ulid
		parsed, err := ulid.ParseStrict(src)
		if err != nil {
			return fmt.Errorf("parse ulid: %w", err)
		}

		*u = ULID(parsed)
		return nil

	case 36:
		// encoded as uuid
		parsed, err := uuid.Parse(src)
		if err != nil {
			return fmt.Errorf("parse ulid as uuid: %w", err)
		}

		*u = ULID(parsed)
		return nil

	default:
		return fmt.Errorf("unsupported length for ulid: %d", len(src))
	}
}

func (u ULID) Value() (driver.Value, error) {
	return u[:], nil
}

//goland:noinspection GoMixedReceiverTypes
func (u *ULID) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		return (*ulid.ULID)(u).UnmarshalBinary(src)

	case string:
		return u.UnmarshalText([]byte(src))

	default:
		return fmt.Errorf("cannot parse ulid from %T", src)
	}
}

var (
	monotonicLock = sync.Mutex{}
	monotonic     = ulid.Monotonic(randSource(), 0)
)

func randSource() *rand.ChaCha8 {
	var seed [32]byte
	if _, err := crand.Read(seed[:]); err != nil {
		panic(err)
	}

	return rand.NewChaCha8(seed)
}
