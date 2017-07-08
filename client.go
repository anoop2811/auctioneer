package auctioneer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"code.cloudfoundry.org/bbs/handlers/middleware"
	"code.cloudfoundry.org/cfhttp"
	"code.cloudfoundry.org/lager"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/tedsuo/rata"
)

//go:generate counterfeiter -o auctioneerfakes/fake_client.go . Client
type Client interface {
	RequestLRPAuctions(logger lager.Logger, ctx context.Context, lrpStart []*LRPStartRequest) error
	RequestTaskAuctions(logger lager.Logger, ctx context.Context, tasks []*TaskStartRequest) error
}

type auctioneerClient struct {
	httpClient         *http.Client
	insecureHTTPClient *http.Client
	url                string
	requireTLS         bool
	traceRequest       middleware.RequestFunc
}

func NewClient(auctioneerURL string, tracer opentracing.Tracer) Client {
	return &auctioneerClient{
		httpClient:   cfhttp.NewClient(),
		url:          auctioneerURL,
		traceRequest: middleware.ToHTTPRequest(tracer),
	}
}

func NewSecureClient(auctioneerURL, caFile, certFile, keyFile string, requireTLS bool, tracer opentracing.Tracer) (Client, error) {
	insecureHTTPClient := cfhttp.NewClient()
	httpClient := cfhttp.NewClient()

	tlsConfig, err := cfhttp.NewTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}

	if tr, ok := httpClient.Transport.(*http.Transport); ok {
		tr.TLSClientConfig = tlsConfig
	} else {
		return nil, errors.New("Invalid transport")
	}

	return &auctioneerClient{
		httpClient:         httpClient,
		insecureHTTPClient: insecureHTTPClient,
		url:                auctioneerURL,
		requireTLS:         requireTLS,
		traceRequest:       middleware.ToHTTPRequest(tracer),
	}, nil
}

func (c *auctioneerClient) RequestLRPAuctions(logger lager.Logger, ctx context.Context, lrpStarts []*LRPStartRequest) error {
	logger = logger.Session("request-lrp-auctions")

	reqGen := rata.NewRequestGenerator(c.url, Routes)
	payload, err := json.Marshal(lrpStarts)
	if err != nil {
		return err
	}

	req, err := reqGen.CreateRequest(CreateLRPAuctionsRoute, rata.Params{}, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req = c.traceRequest(req.WithContext(ctx))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.doRequest(logger, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("http error: status code %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func (c *auctioneerClient) RequestTaskAuctions(logger lager.Logger, ctx context.Context, tasks []*TaskStartRequest) error {
	logger = logger.Session("request-task-auctions")

	reqGen := rata.NewRequestGenerator(c.url, Routes)
	payload, err := json.Marshal(tasks)
	if err != nil {
		return err
	}

	req, err := reqGen.CreateRequest(CreateTaskAuctionsRoute, rata.Params{}, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req = c.traceRequest(req.WithContext(ctx))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.doRequest(logger, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("http error: status code %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func (c *auctioneerClient) doRequest(logger lager.Logger, req *http.Request) (*http.Response, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Fall back to HTTP and try again if we do not require TLS
		if !c.requireTLS && c.insecureHTTPClient != nil {
			logger.Error("retrying-on-http", err)
			req.URL.Scheme = "http"
			return c.insecureHTTPClient.Do(req)
		}
	}
	return resp, err
}
