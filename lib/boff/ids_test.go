package boff

import "testing"

func TestShortId(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  string
	}{
		{"long ulid keeps head and tail", "01M1ETRDSW0QHC9Y6GAVX23ZQW", "01M1ET\u2026X23ZQW"},
		{"prefix survives shortening", "local:01M1ETRDQ8FJ4W2K7ZP0X5NVB1", "local:01M1ET\u2026X5NVB1"},
		{"group id prefix survives", "order:01M1ETRD36144MQC82WZDEF1SA", "order:01M1ET\u2026DEF1SA"},
		{"trace id is shortened like any other id", "005ad836695a73d91e08b44f966e7961", "005ad8\u20266e7961"},
		{"short value is left alone", "mbway", "mbway"},
		{"prefixed short value is left alone", "local:abc", "local:abc"},
		// The threshold is where shortening starts to save characters: twelve in,
		// twelve out is not worth the ellipsis.
		{"exactly at the threshold is left alone", "0123456789ab", "0123456789ab"},
		{"one past the threshold is shortened", "0123456789abc", "012345\u2026789abc"},
		{"empty stays empty", "", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ShortId(c.value); got != c.want {
				t.Errorf("ShortId(%q) = %q, want %q", c.value, got, c.want)
			}
		})
	}
}

func TestShortIdNUsesTheGivenHeadAndTail(t *testing.T) {
	// A 32 character trace id reads better as eight and six.
	const want = "005ad836\u20266e7961"
	if got := ShortIdN("005ad836695a73d91e08b44f966e7961", 8, 6); got != want {
		t.Errorf("ShortIdN = %q, want %q", got, want)
	}
}

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

// An id embedded in a longer text is shortened where it stands; the prose
// around it is untouched.
func TestShortIdsIn(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{
			"id inside a label",
			"Draw 01M1DTGTM47SC9XFPEBAJRRD65",
			"Draw 01M1DT\u2026JRRD65",
		},
		{"prose is left alone", "Payment method", "Payment method"},
		{"short token is left alone", "Item 0", "Item 0"},
		{
			"several ids are all shortened",
			"01M1DTGTM47SC9XFPEBAJRRD65 and 01KXJMFRY454B6BH7Y3TYNWEXR",
			"01M1DT\u2026JRRD65 and 01KXJM\u2026YNWEXR",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ShortIdsIn(c.text); got != c.want {
				t.Errorf("ShortIdsIn(%q) = %q, want %q", c.text, got, c.want)
			}
		})
	}
}
