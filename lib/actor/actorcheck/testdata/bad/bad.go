package bad

import "net/http"

func mayRefund(r *http.Request) bool {
	actorId := r.Header.Get("Actor-Id")

	// the violation: authorization decided from an advisory header
	if actorId == "service-account-backoffice" {
		return true
	}

	return false
}

func approverOf(r *http.Request) string {
	who := r.Header.Get("Actor-Label")

	switch who {
	case "boss@example.com":
		return "approved"
	default:
		return "denied"
	}
}
