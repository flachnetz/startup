package startup_postgres

import (
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/pkg/errors"
	"hash/fnv"
)

func LockWithTransaction(ctx ql.TxContext, key string) error {
	lock, err := lockKey(key)
	if err != nil {
		return errors.WithMessage(err, "lock key")
	}
	if err := ql.Exec(ctx, "SELECT PG_ADVISORY_XACT_LOCK($1)", lock); err != nil {
		return errors.WithMessage(err, "getting advisory lock "+key)
	}

	return nil
}

// TryLockWithTransaction Tries to get a lock for the given key using pg_try_advisory_xact_lock. Returns true,
// if the lock could be acquired.
func TryLockWithTransaction(ctx ql.TxContext, key string) (bool, error) {
	lock, err := lockKey(key)
	if err != nil {
		return false, errors.WithMessage(err, "lock key")
	}

	success, err := ql.Get[bool](ctx, "SELECT PG_TRY_ADVISORY_XACT_LOCK($1)", lock)
	if err != nil {
		return false, errors.WithMessage(err, "getting advisory lock "+key)
	}

	return *success, nil
}

// Postgres locks on integers, not on strings. As such we just hash the string into
// some 63 bit integer using the pretty good fnv hash. Conflicts are _pretty_ rare and
// in our case normally not a problem. If there would be a conflict, then two processes
// are waiting on the same lock, which probably just means a little longer waiting time.
func lockKey(key string) (uint64, error) {
	h := fnv.New64a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, errors.WithMessage(err, "hashing key")
	}

	return h.Sum64() & ^(uint64(1) << 63), nil
}
