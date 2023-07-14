package startup_failpoints

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	logger             *logrus.Entry
	devMode            bool
	failPointsLock     sync.RWMutex
	failPoints         []FailPoint
	errorLookup        map[string]FailPointError
	failPointLocations map[FailPointLocation]*FailPoint
}

func NewFailPointService(fps []FailPoint, codeLocations []FailPointLocation, devMode bool) *FailPointService {
	f := &FailPointService{
		logger:             logrus.WithField("prefix", "failpoints"),
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
		point := f.failPoints[0]
		f.failPointLocations[v] = &point
	}
	return f
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
			if len(fp.FilterTags) > 0 && !containsOneOf(fp, filterTags) {
				return nil
			}

			var timeoutError timeoutError
			if errors.As(fp.Error, &timeoutError) {
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
	fp.Error = err
	fp.IsActive = req.Active
	fp.FilterTags = nil
	if req.FilterTags != "" {
		fp.FilterTags = strings.Split(req.FilterTags, ",")
		for i, part := range fp.FilterTags {
			fp.FilterTags[i] = strings.ToLower(strings.TrimSpace(part))
		}
	}
	f.logger.Infof("set failpoint location '%s' with error '%s' to state active:%v", req.CodeLocationPointName, req.FailPointErrorName, req.Active)
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

func (f *FailPointService) HandleFailPointPage(updateFailPointsEndpoint string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		err := renderIndex(writer, TemplateResponse{
			UpdateFailPointsEndpoint: updateFailPointsEndpoint,
			FailPoints:               f.GetFailPoints(),
			FailPointLocations:       f.GetFailPointLocations(),
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
