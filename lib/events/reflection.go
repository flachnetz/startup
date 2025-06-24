package events

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func nameOf(event Event) string {
	// try to take the Name of the schema
	var schema struct{ Name string }
	if json.Unmarshal([]byte(event.Schema()), &schema) == nil && schema.Name != "" {
		return schema.Name
	}

	// get the event class
	eventType := reflect.ValueOf(event).Type()
	for eventType.Kind() == reflect.Ptr || eventType.Kind() == reflect.Interface {
		eventType = eventType.Elem()
	}

	// and take the name of it
	name := eventType.Name()
	if name != "" {
		return name
	}

	return "GoAvroEvent"
}

var eventInterfaceType = reflect.TypeOf((*Event)(nil)).Elem()

// derefEventType ensures that the given event type is dereferenced until
// it points directly to an event struct
func derefEventType(eventType reflect.Type) reflect.Type {
	for eventType.Kind() == reflect.Ptr {
		eventType = eventType.Elem()
	}

	if !reflect.PointerTo(eventType).Implements(eventInterfaceType) {
		panic(
			fmt.Sprintf("Pointer to type '%s' does not implement 'Event' interface", eventType),
		)
	}

	return eventType
}
