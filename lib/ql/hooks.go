package ql

type hooks struct {
	onCommit []Action
}

func (oc *hooks) OnCommit(action Action) {
	oc.onCommit = append(oc.onCommit, action)
}

func (oc *hooks) RunOnCommit() {
	for _, action := range oc.onCommit {
		action()
	}
}
