package boff

import "testing"

func TestFormatMoney(t *testing.T) {
	cases := []struct {
		minor    any
		currency string
		want     string
	}{
		{1234, "EUR", "12.34 EUR"},
		{int64(5), "EUR", "0.05 EUR"},
		{int32(100), "USD", "1.00 USD"},
		{0, "EUR", "0.00 EUR"},
		{-1234, "EUR", "-12.34 EUR"},
		{-5, "EUR", "-0.05 EUR"},
	}

	for _, c := range cases {
		got, err := formatMoney(c.minor, c.currency)
		if err != nil {
			t.Errorf("formatMoney(%v, %q): %v", c.minor, c.currency, err)
			continue
		}
		if got != c.want {
			t.Errorf("formatMoney(%v, %q) = %q, want %q", c.minor, c.currency, got, c.want)
		}
	}

	if _, err := formatMoney("nope", "EUR"); err == nil {
		t.Error("formatMoney did not reject a non-integer amount")
	}
}
