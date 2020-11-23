package startup_http

import (
	"bytes"
	"crypto/tls"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_tracing"
)

func RetryableHttpClient(logger *logrus.Entry, client *http.Client, debug bool) *retryablehttp.Client {
	httpClient := retryablehttp.NewClient()

	if client == nil {
		client = startup_tracing.WithSpanPropagation(&http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		})
	}

	httpClient.HTTPClient = client

	if debug {
		httpClient.RequestLogHook = func(l retryablehttp.Logger, request *http.Request, i int) {
			if request != nil {
				if i == 0 && request.Body != nil {
					rBody, _ := ioutil.ReadAll(request.Body)
					logger.Debugf("%s %s:\n %+v", request.Method, request.URL, string(rBody))
					request.Body = ioutil.NopCloser(bytes.NewReader(rBody))
				} else {
					logger.Debugf("%s %s (%d. try)", request.Method, request.URL, i+1)
				}
			}
		}

		httpClient.ResponseLogHook = func(l retryablehttp.Logger, resp *http.Response) {
			body, _ := ioutil.ReadAll(resp.Body)
			logger.Debugf("Response:\n %s", string(body))
			resp.Body = ioutil.NopCloser(bytes.NewReader(body))
		}
	}

	httpClient.ErrorHandler = func(resp *http.Response, err error, numTries int) (*http.Response, error) {
		if resp != nil {
			body, _ := ioutil.ReadAll(resp.Body)
			logger.Errorf("Response:\n %s", string(body))
			resp.Body = ioutil.NopCloser(bytes.NewReader(body))
		} else {
			logger.Errorf("Response: %v", err)
		}
		return resp, err
	}

	httpClient.RetryMax = 1

	if debug {
		httpClient.Logger = logger
	} else {
		httpClient.Logger = nil
	}

	return httpClient
}

func DoRequest(httpClient *retryablehttp.Client, httpReq *retryablehttp.Request, auth string, errorParser func([]byte) error) ([]byte, error) {

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", auth)

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, errors.Wrapf(err, "doRequest: cannot do request for %s", httpReq.URL.String())
	}

	defer startup_base.Close(resp.Body, "cannot close body")

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "doRequest: cannot read body from request %s", httpReq.URL.String())
	}

	if resp.StatusCode/100 != 2 {
		if len(body) > 0 && errorParser != nil {
			return nil, errorParser(body)
		} else {
			return nil, errors.Wrapf(err, "doRequest: request: %s status %d", httpReq.URL.String(), resp.StatusCode)
		}

	}

	return body, nil
}
