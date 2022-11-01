package set

// Set represents a set.
// It is implemented as a thin wrapper over a map with a zero sized value.
type Set[T comparable] struct {
	values map[T]struct{}
}

// WithCapacity creates a new set with the given capacity.
func WithCapacity[T comparable](cap int) Set[T] {
	return Set[T]{values: make(map[T]struct{}, cap)}
}

// FromValues creates a new set from the given values. If the provided values contain
// duplicates, the set will only contain one of instance of each value.
func FromValues[T comparable](values []T) Set[T] {
	set := WithCapacity[T](len(values))

	for _, value := range values {
		set.Add(value)
	}

	return set
}

// Len returns the number of values in the Set.
func (s *Set[T]) Len() int {
	return len(s.values)
}

// Contains returns true iff the value is part of the Set.
func (s *Set[T]) Contains(value T) bool {
	_, ok := s.values[value]
	return ok
}

// Add adds the given value to the set. Returns true,
// iff the value was not part of the set and was actually added.
func (s *Set[T]) Add(value T) bool {
	_, ok := s.values[value]
	if ok {
		return false
	}

	if s.values == nil {
		s.values = map[T]struct{}{}
	}

	s.values[value] = struct{}{}

	return true
}

// Remove removes the given value from the set. Returns true,
// iff the value was part of the set and got removed.
func (s *Set[T]) Remove(value T) bool {
	_, ok := s.values[value]
	if ok {
		delete(s.values, value)
		return true
	}

	return false
}
