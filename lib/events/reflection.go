package events

import (
	"fmt"
	"reflect"
)

var eventInterfaceType = reflect.TypeFor[Event]()

// derefEventType ensures that the given event type is dereferenced until
// it points directly to an event struct
func derefEventType(eventType reflect.Type) reflect.Type {
	for eventType.Kind() == reflect.Pointer {
		eventType = eventType.Elem()
	}

	if !reflect.PointerTo(eventType).Implements(eventInterfaceType) {
		panic(
			fmt.Sprintf("Pointer to type %q does not implement 'Event' interface", eventType),
		)
	}

	return eventType
}
