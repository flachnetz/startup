package startup_failpoints

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

type FailPointLocation string

type timeoutError struct {
	name    string
	timeout time.Duration
	forever bool
}

func (t timeoutError) Name() string {
	return t.name
}

func (t timeoutError) Error() string {
	return t.name
}

var timeoutErrors = []FailPointError{
	timeoutError{"timeout_1_second", 1 * time.Second, false},
	timeoutError{"timeout_3_seconds", 3 * time.Second, false},
	timeoutError{"timeout_5_seconds", 5 * time.Second, false},
	timeoutError{"timeout_10_seconds", 10 * time.Second, false},
	timeoutError{"timeout_30_seconds", 30 * time.Second, false},
	timeoutError{"timeout_forever", 365 * 24 * time.Hour, true},
}

type FailPointError interface {
	error
}

type FailPoint struct {
	Error    FailPointError `json:"error"`
	IsActive bool           `json:"isActive"`
}

type FailPointRequest struct {
	CodeLocationPointName FailPointLocation `json:"codeLocationPointName"`
	FailPointErrorName    string            `json:"failPointCode"`
	Active                bool              `json:"active"`
}

type FailPointService struct {
	logger             *logrus.Entry
	devMode            bool
	failPointsLock     sync.RWMutex
	failPoints         []FailPoint
	errorLookup        map[string]FailPointError
	failPointLocations map[FailPointLocation]*FailPoint
}

func NewFailPointService(failPoints []FailPoint, codeLocations []FailPointLocation, defaultError FailPointError, devMode bool) *FailPointService {
	for _, pointError := range timeoutErrors {
		failPoints = append(failPoints, NewFailPoint(pointError))
	}
	failPoints = append(failPoints)
	f := &FailPointService{
		logger:             logrus.WithField("prefix", "failpoints"),
		devMode:            devMode,
		failPointsLock:     sync.RWMutex{},
		failPoints:         failPoints,
		failPointLocations: make(map[FailPointLocation]*FailPoint),
		errorLookup:        make(map[string]FailPointError),
	}
	for _, v := range codeLocations {
		point := NewFailPoint(defaultError)
		f.failPointLocations[v] = &point
	}
	for _, fp := range failPoints {
		f.errorLookup[fp.Error.Error()] = fp.Error
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
	for _, fp := range f.failPoints {
		resp = append(resp, fp)
	}
	return resp
}

func (f *FailPointService) ReturnErrorIfFailPointActive(ctx context.Context, location FailPointLocation) error {
	if f.devMode {
		f.failPointsLock.Lock()
		fp, exists := f.failPointLocations[location]
		f.failPointsLock.Unlock()
		if exists && fp.IsActive {
			var timeoutError timeoutError
			if errors.As(fp.Error, &timeoutError) {
				// we just wait as long as the client keeps the connection open
				if timeoutError.forever {
					<-ctx.Done()
				}
				time.Sleep(timeoutError.timeout)
				return nil
			}

			return fp.Error //rgs2.RgsErrorf(fp.Error, "%s - %s", location, fp)
		}
	}
	return nil
}

func (f *FailPointService) UpdateFailPoint(req FailPointRequest) error {
	if !f.devMode {
		return errors.New("only allowed in dev mode")
	}
	f.failPointsLock.Lock()
	defer f.failPointsLock.Unlock()
	fp := f.failPointLocations[req.CodeLocationPointName]
	err, ok := f.errorLookup[req.FailPointErrorName]
	if !ok {
		return errors.New("cannot find error for " + req.FailPointErrorName)
	}
	fp.Error = err
	fp.IsActive = req.Active
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
