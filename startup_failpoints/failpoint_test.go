package startup_failpoints

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	locA FailPointLocation = "a"
	locB FailPointLocation = "b"
)

func newTestService() *FailPointService {
	return NewFailPointService([]FailPoint{
		{Error: errors.New("everywhere"), ErrorName: "everywhere"},
		{Error: errors.New("only a"), ErrorName: "only_a", Locations: []FailPointLocation{locA}},
	}, []FailPointLocation{locA, locB}, true)
}

func names(fps []FailPoint) []string {
	out := make([]string, 0, len(fps))
	for _, fp := range fps {
		out = append(out, fp.ErrorName)
	}
	return out
}

func TestFailPointsForOffersALocationScopedErrorOnlyThere(t *testing.T) {
	f := newTestService()

	assert.Contains(t, names(f.FailPointsFor(locA)), "only_a")
	assert.NotContains(t, names(f.FailPointsFor(locB)), "only_a")

	// An unscoped error stays available at every location.
	assert.Contains(t, names(f.FailPointsFor(locA)), "everywhere")
	assert.Contains(t, names(f.FailPointsFor(locB)), "everywhere")

	// The full list is unchanged for callers rendering their own page.
	assert.Contains(t, names(f.GetFailPoints()), "only_a")
}

func TestUpdateFailPointRejectsAnErrorNotOfferedAtTheLocation(t *testing.T) {
	f := newTestService()

	require.NoError(t, f.UpdateFailPoint(FailPointRequest{
		CodeLocationPointName: locA, FailPointErrorName: "only_a", Active: true,
	}))

	err := f.UpdateFailPoint(FailPointRequest{
		CodeLocationPointName: locB, FailPointErrorName: "only_a", Active: true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not offered at b")
}

func TestDefaultSelectionIsOfferedAtItsLocation(t *testing.T) {
	// "only_a" sorts before "everywhere", so location b must not default to it.
	f := newTestService()

	for location, fp := range f.GetFailPointLocations() {
		assert.Contains(t, names(f.FailPointsFor(location)), fp.ErrorName,
			"default of %s must be selectable there", location)
	}
}

func TestFailPointsByLocationCoversEveryLocation(t *testing.T) {
	f := newTestService()

	byLocation := f.FailPointsByLocation()
	require.Len(t, byLocation, 2)
	assert.ElementsMatch(t, []string{"only_a", "everywhere"}, dedupeSelectable(byLocation[locA]))
	assert.NotContains(t, names(byLocation[locB]), "only_a")
}

// dedupeSelectable keeps only the two errors this test declared, dropping the
// timeout and internal-error entries the constructor adds.
func dedupeSelectable(fps []FailPoint) []string {
	var out []string
	for _, name := range names(fps) {
		if name == "only_a" || name == "everywhere" {
			out = append(out, name)
		}
	}
	return out
}

func TestFailPointPageRendersPerLocationControlsAndPlaywrightSnippet(t *testing.T) {
	f := newTestService()
	require.NoError(t, f.UpdateFailPoint(FailPointRequest{
		CodeLocationPointName: locA, FailPointErrorName: "only_a", Active: true, FilterTags: "Tag1, tag2",
	}))

	rec := httptest.NewRecorder()
	f.HandleFailPointPage("/internal/failpoints")(rec, httptest.NewRequest(http.MethodPut, "/failpoints", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	// Controls exist per location, with the armed state reflected.
	assert.Contains(t, body, `id="select__a"`)
	assert.Contains(t, body, `id="cb__a"`)
	assert.Contains(t, body, `id="ft__a"`)
	assert.Contains(t, body, `value="tag1,tag2"`)
	assert.Contains(t, body, `id="select__b"`)

	// The tester-facing snippet target and the endpoint it PUTs to are rendered.
	assert.Contains(t, body, `id="snippet__a"`)
	assert.Contains(t, body, "/internal/failpoints")
}
