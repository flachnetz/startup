package boff

import "testing"

func TestIsId(t *testing.T) {
	ids := []string{"01M1ETRDSW0QHC9Y6GAVX23ZQW", "local:01M1ETRDQ8FJ4W2K7ZP0X5NVB1", "evt_azyhxvklgwqeffdzf"}
	for _, value := range ids {
		if !IsId(value) {
			t.Errorf("IsId(%q) = false, want true", value)
		}
	}

	// Prose, amounts and short values are not ids: shortening them loses meaning.
	prose := []string{"showmethemoney-pt", "2.50 EUR", "Maquina de Dinheiro (1)", "PAID", "2026-09-01 15:52:41 UTC"}
	for _, value := range prose {
		if IsId(value) {
			t.Errorf("IsId(%q) = true, want false", value)
		}
	}
}

func TestAmountDetection(t *testing.T) {
	for _, value := range []string{"2.50 EUR", "-12.34 EUR", "0,00 EUR", "5.00"} {
		if !IsAmount(value) {
			t.Errorf("IsAmount(%q) = false, want true", value)
		}
	}
	if IsAmount("2.5 EUR") {
		t.Error("IsAmount accepted a value with one decimal")
	}

	for _, value := range []string{"0.00 EUR", "0,00 EUR", "0.00"} {
		if !IsZeroAmount(value) {
			t.Errorf("IsZeroAmount(%q) = false, want true", value)
		}
	}
	if IsZeroAmount("10.00 EUR") {
		t.Error("IsZeroAmount accepted a non-zero amount")
	}
}

func TestJSONFieldCount(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    int
	}{
		{"object counts top level fields", `{"a":1,"b":{"c":2,"d":3},"e":4}`, 3},
		{"array counts elements", `[1,2,3]`, 3},
		{"empty object has no fields", `{}`, 0},
		{"empty payload has no fields", "", 0},
		{"invalid json has no fields", "not json", 0},
		{"scalar has no fields", `"just a string"`, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := JSONFieldCount(c.payload); got != c.want {
				t.Errorf("JSONFieldCount(%q) = %d, want %d", c.payload, got, c.want)
			}
		})
	}
}

func TestPipToneMapsOnlyTheSeverityVocabulary(t *testing.T) {
	cases := map[string]string{
		"success": "pip pip-ok",
		"danger":  "pip pip-err",
		"warning": "pip pip-warn",
		"info":    "pip",
		"":        "pip",
	}

	for tone, want := range cases {
		if got := pipClass(tone); got != want {
			t.Errorf("pipClass(%q) = %q, want %q", tone, got, want)
		}
	}

	if got := statusClass("success"); got != "status status-ok" {
		t.Errorf("statusClass(success) = %q", got)
	}
	if got := statusClass("info"); got != "status" {
		t.Errorf("statusClass(info) = %q", got)
	}
}
