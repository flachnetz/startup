package schema

type noopRegistry struct{}

func NewNoopRegistry() Registry {
	return noopRegistry{}
}

func (noopRegistry) Get(key string) (string, error) {
	return "", nil
}

func (noopRegistry) Set(schema string) (string, error) {
	return "", nil
}

func (noopRegistry) Close() error {
	return nil
}
