package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/karust/openserp/agentcontext"
)

type contextPayload struct {
	agentcontext.Request
	Engines []string `json:"engines,omitempty"`
	Lang    string   `json:"lang,omitempty"`
	Region  string   `json:"region,omitempty"`
	Mode    string   `json:"extract_mode,omitempty"`
}

type contextResponse struct {
	*agentcontext.Response
	Meta contextMeta `json:"meta"`
}

type contextMeta struct {
	RequestID string `json:"request_id"`
	TookMs    int64  `json:"took_ms"`
}

func (s *Server) handleContext(c *fiber.Ctx) error {
	started := time.Now()
	cfg := s.opts.Extract.Normalized()
	if !cfg.Enabled {
		return &APIError{HTTPStatus: 404, ErrorCode: "not_found", Message: "Extraction is disabled"}
	}
	if len(c.Body()) > 16384 {
		return errInvalidParam("context request must be at most 16384 bytes")
	}
	var body contextPayload
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		return errInvalidParam("invalid context JSON body")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errInvalidParam("expected one JSON object")
	}
	if err := body.Normalize(); err != nil {
		return errInvalidParam(err.Error())
	}
	if len(body.Engines) > 3 {
		return errInvalidParam("at most 3 engines are allowed")
	}
	if len(body.Lang) > 32 || len(body.Region) > 128 {
		return errInvalidParam("invalid language or region")
	}
	// Reuse extraction's proxy-header validation and SSRF-protected fetchers.
	base, err := s.baseExtractRequest(c, extractPayload{Mode: body.Mode, Lang: body.Lang}, cfg)
	if err != nil {
		return err
	}
	proxyOverride, err := NormalizeProxyRequestOverride(c.Get("X-Use-Proxy"))
	if err != nil {
		return errInvalidParam("invalid proxy override")
	}
	baseQuery := Query{LangCode: body.Lang, Region: body.Region, Limit: 10, Filter: true, ProxyURL: base.ProxyURL, ProxyOverride: proxyOverride,
		ProxyCountry:   strings.ToLower(strings.TrimSpace(c.Get("X-Proxy-Country"))),
		ProxyClass:     strings.ToLower(strings.TrimSpace(c.Get("X-Proxy-Class"))),
		ProxyProvider:  strings.ToLower(strings.TrimSpace(c.Get("X-Proxy-Provider"))),
		ProxySessionID: strings.TrimSpace(c.Get("X-Proxy-Session-ID"))}
	ctx := withRequestUsage(c.UserContext(), "context")
	c.SetUserContext(ctx)
	defer setNetworkBytesHeader(c, ctx)
	defer setBrowserProfileHeader(c, ctx)
	engines := s.searchEngines
	if len(body.Engines) > 0 {
		engines = nil
		seen := map[string]bool{}
		for _, name := range body.Engines {
			name = resolveEngineAlias(strings.ToLower(strings.TrimSpace(name)))
			var matched SearchEngine
			for _, engine := range s.searchEngines {
				if strings.EqualFold(engine.Name(), name) {
					matched = engine
					break
				}
			}
			if matched == nil {
				return errInvalidParam("unknown context engine")
			}
			if !seen[name] {
				engines = append(engines, matched)
				seen[name] = true
			}
		}
	} else if len(engines) > 2 {
		engines = engines[:2]
	}
	if len(engines) == 0 {
		return errInvalidParam("no search engines configured")
	}
	// One shared wall-clock budget includes search, fetch, and selection.
	timeout := s.opts.MegaTimeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	extractor := s.newExtractor()
	search := func(ctx context.Context, text string) (agentcontext.SearchResult, error) {
		// Do not access Fiber's request from concurrent search workers.
		q := baseQuery
		q.Text = text
		raw, _, failures := s.resilient.searchAllParallelDetailed(ctx, q, engines)
		result := agentcontext.SearchResult{}
		for range failures {
			result.Failures = append(result.Failures, agentcontext.Failure{Stage: "search", Query: text, Code: "engine_unavailable"})
		}
		for _, hit := range raw {
			if hit.Ad || hit.Rank <= 0 || hit.Rank > 10 || (hit.Type != "" && hit.Type != ResultTypeOrganic) {
				continue
			}
			result.Hits = append(result.Hits, agentcontext.Hit{URL: hit.URL, Title: hit.Title, Rank: hit.Rank, Engine: hit.Engine})
		}
		return result, nil
	}
	fetchSlots := make(chan struct{}, min(2, cfg.MaxConcurrent))
	fetch := func(ctx context.Context, url string) (agentcontext.Document, error) {
		select {
		case fetchSlots <- struct{}{}:
		case <-ctx.Done():
			return agentcontext.Document{}, ctx.Err()
		}
		defer func() { <-fetchSlots }()
		req := base
		req.URL = url
		doc, err := extractor.Extract(ctx, req)
		if err != nil {
			WithRequest(ctx).WithError(err).Warn("Context source extraction failed")
			return agentcontext.Document{}, err
		}
		if !ExtractedContentLooksUseful(doc.Markdown) {
			return agentcontext.Document{}, errors.New("insufficient page content")
		}
		return agentcontext.Document{Markdown: doc.Markdown, Title: doc.Title, FetchedAt: doc.Meta.FetchedAt}, nil
	}
	result, err := agentcontext.Build(ctx, body.Request, search, fetch)
	if err != nil {
		return &APIError{HTTPStatus: 500, ErrorCode: "context_failed", Message: "Failed to build context"}
	}
	return c.JSON(contextResponse{Response: result, Meta: contextMeta{RequestID: RequestIDFromContext(ctx), TookMs: time.Since(started).Milliseconds()}})
}
