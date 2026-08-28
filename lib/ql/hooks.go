package ql

type hooks struct {
	onCommit     []Action
	beforeCommit []FallibleAction
	onDone       []Action
}

func (oc *hooks) OnCommit(action Action) {
	oc.onCommit = append(oc.onCommit, action)
}

func (oc *hooks) BeforeCommit(action FallibleAction) {
	oc.beforeCommit = append(oc.beforeCommit, action)
}

// RunBeforeCommit runs the pending before-commit hooks against the still open
// transaction. A hook may register another one (a flush that tracks something
// itself), so the list is drained until empty. The first error aborts, and the
// caller rolls the transaction back.
func (oc *hooks) RunBeforeCommit(ctx TxContext) error {
	for len(oc.beforeCommit) > 0 {
		pending := oc.beforeCommit
		oc.beforeCommit = nil

		for _, action := range pending {
			if err := action(ctx); err != nil {
				oc.beforeCommit = nil
				return err
			}
		}
	}

	return nil
}

func (oc *hooks) RunOnCommit() {
	for _, action := range oc.onCommit {
		action()
	}

	oc.onCommit = oc.onCommit[:0]
}

func (oc *hooks) OnDone(action Action) {
	oc.onDone = append(oc.onDone, action)
}

// RunOnDone runs the cleanup hooks, whatever the transaction's outcome was.
func (oc *hooks) RunOnDone() {
	for _, action := range oc.onDone {
		action()
	}

	oc.onDone = nil
}
