package startup_failpoints

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
)

type FailPointLocation string

type FailPointError interface {
	error
}

type FailPoint struct {
	Error FailPointError `json:"error"`
	// when empty, Error.String() will be taken
	ErrorName  string   `json:"errorName"`
	IsActive   bool     `json:"isActive"`
	FilterTags []string `json:"filterTags"`

	// Locations restricts the failure to those code locations: it is only offered
	// there and can only be armed there. Empty means every location, which is what
	// a generic failure (a timeout, a 500) wants. A service whose locations are
	// different peers uses it to keep one peer's product errors out of another
	// peer's dropdown.
	Locations []FailPointLocation `json:"locations,omitempty"`
}

// offeredAt reports whether the fail point can be armed at location.
func (fp FailPoint) offeredAt(location FailPointLocation) bool {
	return len(fp.Locations) == 0 || slices.Contains(fp.Locations, location)
}

type timeoutError struct {
	name    string
	timeout time.Duration
	forever bool
}

func (t timeoutError) Error() string {
	return t.name
}

func timeoutErrorFailPoints(durations []int) []FailPoint {
	var result []FailPoint
	for _, v := range durations {
		name := fmt.Sprintf("timeout_%02d_seconds", v*int(time.Second.Seconds()))
		result = append(result, FailPoint{
			Error: timeoutError{
				name:    name,
				timeout: time.Duration(v * int(time.Second)),
				forever: false,
			},
			ErrorName: name,
			IsActive:  false,
		})
	}
	return result
}

type FailPointRequest struct {
	CodeLocationPointName FailPointLocation `json:"codeLocationPointName"`
	FailPointErrorName    string            `json:"failPointCode"`
	Active                bool              `json:"active"`
	FilterTags            string            `json:"filterTags"`
}

type FailPointService struct {
	logger             *slog.Logger
	devMode            bool
	failPointsLock     sync.RWMutex
	failPoints         []FailPoint
	errorLookup        map[string]FailPointError
	failPointLocations map[FailPointLocation]*FailPoint
}

func NewFailPointService(fps []FailPoint, codeLocations []FailPointLocation, devMode bool) *FailPointService {
	f := &FailPointService{
		logger:             slog.With(slog.String("prefix", "failpoints")),
		devMode:            devMode,
		failPointsLock:     sync.RWMutex{},
		failPoints:         []FailPoint{},
		failPointLocations: make(map[FailPointLocation]*FailPoint),
		errorLookup:        make(map[string]FailPointError),
	}

	fps = append(fps, timeoutErrorFailPoints([]int{1, 3, 5, 10, 30, 365 * 24 * 60 * 60})...)
	fps = append(fps, FailPoint{Error: errors.New("internal server error"), ErrorName: "internal server error", IsActive: false})

	// fix error name and copy fail points
	for _, fp := range fps {
		if fp.ErrorName == "" {
			fp.ErrorName = fp.Error.Error()
		}
		f.failPoints = append(f.failPoints, fp)
		f.errorLookup[fp.ErrorName] = fp.Error
	}
	sort.Slice(f.failPoints, func(i, j int) bool {
		return f.failPoints[i].ErrorName < f.failPoints[j].ErrorName
	})

	for _, v := range codeLocations {
		// The default selection has to be one the location actually offers, otherwise
		// the page shows a value its own dropdown does not contain.
		point := f.failPoints[0]
		for _, fp := range f.failPoints {
			if fp.offeredAt(v) {
				point = fp
				break
			}
		}
		f.failPointLocations[v] = &point
	}
	return f
}

// FailPointsFor returns the fail points offered at location, in the order the
// page should show them.
func (f *FailPointService) FailPointsFor(location FailPointLocation) []FailPoint {
	f.failPointsLock.RLock()
	defer f.failPointsLock.RUnlock()

	resp := make([]FailPoint, 0, len(f.failPoints))
	for _, fp := range f.failPoints {
		if fp.offeredAt(location) {
			resp = append(resp, fp)
		}
	}
	return resp
}

// FailPointsByLocation is the per-location selection the page renders.
func (f *FailPointService) FailPointsByLocation() map[FailPointLocation][]FailPoint {
	resp := make(map[FailPointLocation][]FailPoint, len(f.failPointLocations))
	for location := range f.GetFailPointLocations() {
		resp[location] = f.FailPointsFor(location)
	}
	return resp
}

func (f *FailPointService) NewFailPointRequest() FailPointRequest {
	return FailPointRequest{}
}

func (f *FailPointService) GetFailPointLocations() map[FailPointLocation]FailPoint {
	f.failPointsLock.Lock()
	defer f.failPointsLock.Unlock()
	resp := make(map[FailPointLocation]FailPoint, len(f.failPointLocations))
	for k, v := range f.failPointLocations {
		resp[k] = *v
	}
	return resp
}

func (f *FailPointService) GetFailPoints() []FailPoint {
	f.failPointsLock.Lock()
	defer f.failPointsLock.Unlock()
	var resp []FailPoint
	resp = append(resp, f.failPoints...)
	return resp
}

// ReturnErrorIfFailPointActive returns an error if the failpoint is active.
// If the failpoint is not active, it returns nil.
// filterTags is a list of tags that can be used to filter the failpoint as a condition for activation.
func (f *FailPointService) ReturnErrorIfFailPointActive(ctx context.Context, location FailPointLocation, filterTags ...string) error {
	if f.devMode {
		f.failPointsLock.Lock()
		fp, exists := f.failPointLocations[location]
		f.failPointsLock.Unlock()
		if exists && fp.IsActive {
			// if filterTags are set, we only return an error if the failpoint has one of the filter tags
			if len(fp.FilterTags) > 0 {
				f.logger.Debug("checking whether failpoint has one of the filter tags", slog.String("location", string(location)), slog.String("filterTags", strings.Join(filterTags, ",")))
				// no match, return no error
				if !containsOneOf(fp, filterTags) {
					return nil
				}
			}

			if timeoutError, ok := errors.AsType[timeoutError](fp.Error); ok {
				// we just wait as long as the client keeps the connection open
				if timeoutError.forever {
					<-ctx.Done()
				}
				time.Sleep(timeoutError.timeout)
				return nil
			}

			return fp.Error // rgs2.RgsErrorf(fp.Error, "%s - %s", location, fp)
		}
	}
	return nil
}

// containsOneOf returns true if the failpoint has one of the filter tags
func containsOneOf(fp *FailPoint, filterTags []string) bool {
	if len(fp.FilterTags) > 0 {
		for _, tag := range filterTags {
			if slices.Contains(fp.FilterTags, strings.ToLower(tag)) {
				return true
			}
		}
	}
	return false
}

func (f *FailPointService) UpdateFailPoint(req FailPointRequest) error {
	if !f.devMode {
		return errors.New("only allowed in dev mode")
	}
	f.failPointsLock.Lock()
	defer f.failPointsLock.Unlock()
	fp, ok := f.failPointLocations[req.CodeLocationPointName]
	if !ok {
		return errors.New("cannot find failpoint location for " + string(req.CodeLocationPointName))
	}

	err, ok := f.errorLookup[req.FailPointErrorName]
	if !ok {
		return errors.New("cannot find error for " + req.FailPointErrorName)
	}
	if !f.offeredAt(req.FailPointErrorName, req.CodeLocationPointName) {
		return errors.New(req.FailPointErrorName + " is not offered at " + string(req.CodeLocationPointName))
	}
	fp.Error = err
	fp.ErrorName = req.FailPointErrorName
	fp.IsActive = req.Active
	fp.FilterTags = nil
	if req.FilterTags != "" {
		fp.FilterTags = strings.Split(req.FilterTags, ",")
		for i, part := range fp.FilterTags {
			fp.FilterTags[i] = strings.ToLower(strings.TrimSpace(part))
		}
	}
	f.logger.Info("set failpoint location", slog.String("location", string(req.CodeLocationPointName)), slog.String("error", req.FailPointErrorName), slog.Bool("active", req.Active))
	return nil
}

func (f *FailPointService) UpdateFailPointHandlerFunc() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var req FailPointRequest
		err := json.NewDecoder(request.Body).Decode(&req)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		err = f.UpdateFailPoint(req)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}
}

// offeredAt reports whether the error named errorName may be armed at location.
// The caller holds the lock.
func (f *FailPointService) offeredAt(errorName string, location FailPointLocation) bool {
	for _, fp := range f.failPoints {
		if fp.ErrorName == errorName {
			return fp.offeredAt(location)
		}
	}
	return false
}

func (f *FailPointService) HandleFailPointPage(updateFailPointsEndpoint string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		err := renderIndex(writer, TemplateResponse{
			UpdateFailPointsEndpoint: updateFailPointsEndpoint,
			FailPoints:               f.GetFailPoints(),
			FailPointLocations:       f.GetFailPointLocations(),
			FailPointsByLocation:     f.FailPointsByLocation(),
		})
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func NewFailPoint(err FailPointError) FailPoint {
	return FailPoint{
		Error:    err,
		IsActive: false,
	}
}
