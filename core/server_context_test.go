package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karust/openserp/agentcontext"
)

func contextRequest(t *testing.T, s *Server, body string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/context", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestContextEndpointSearchFetchAndCite(t *testing.T) {
	var fetches atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		_, _ = w.Write([]byte(`<html><head><title>Replication restrictions</title></head><body><article><h1>Logical replication</h1><p>Logical replication does not replicate DDL changes or sequence values. Schema changes must be applied separately on subscribers.</p></article></body></html>`))
	}))
	defer target.Close()
	engine := &engineMock{name: "google", initialized: true, searchFn: func(_ context.Context, q Query) ([]SearchResult, error) {
		if q.Limit != 10 || q.LangCode != "en" || q.Region != "us" {
			t.Errorf("query settings: %+v", q)
		}
		return []SearchResult{{URL: target.URL + "/docs", Title: "Docs", Rank: 1}, {URL: target.URL + "/docs#section", Rank: 2}, {URL: target.URL + "/ad", Rank: 1, Ad: true}}, nil
	}}
	opts := DefaultServerOptions()
	opts.Extract.AllowPrivateNetworks = true
	opts.Extract.DefaultMode = "fast"
	s := NewServerWithOptions("127.0.0.1", 0, opts, engine)
	resp := contextRequest(t, s, `{"query":"logical replication restrictions","queries":["replication DDL sequences"],"engines":["google"],"lang":"en","region":"us","max_tokens":256}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		agentcontext.Response
		Meta struct {
			RequestID string `json:"request_id"`
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Partial || !strings.Contains(out.Context, "DDL") || len(out.Sources) != 1 || out.Meta.RequestID == "" {
		t.Fatalf("response %+v", out)
	}
	if fetches.Load() != 1 || out.Usage.Searches != 2 || out.Usage.ContextTokens > 256 {
		t.Fatalf("usage %+v, fetches %d", out.Usage, fetches.Load())
	}
}

func TestContextEndpointValidationBeforeSearch(t *testing.T) {
	engine := &engineMock{name: "google", initialized: true, searchFn: func(context.Context, Query) ([]SearchResult, error) {
		t.Error("searched for invalid request")
		return nil, nil
	}}
	s := NewServerWithOptions("127.0.0.1", 0, DefaultServerOptions(), engine)
	for _, body := range []string{`{`, `{}`, `null`, `{"query":"x","unknown":true}`, `{"query":"x"}{}`, `{"query":"x","max_tokens":-1}`, `{"query":"x","engines":["unknown"]}`, `{"query":"x","engines":["google,unknown"]}`, `{"query":"x","engines":[""]}`, `{"query":"x","extract_mode":"invalid"}`, `{"query":"x","engines":["google","bing","duck","yandex"]}`} {
		resp := contextRequest(t, s, body)
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", body, resp.StatusCode)
		}
	}
}

func TestContextEndpointPrivateTargetsRemainBlocked(t *testing.T) {
	var fetched atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fetched.Store(true) }))
	defer target.Close()
	engine := &engineMock{name: "google", initialized: true, searchFn: func(context.Context, Query) ([]SearchResult, error) {
		return []SearchResult{{URL: target.URL, Rank: 1}}, nil
	}}
	opts := DefaultServerOptions()
	opts.Extract.DefaultMode = "fast"
	s := NewServerWithOptions("127.0.0.1", 0, opts, engine)
	resp := contextRequest(t, s, `{"query":"replication"}`)
	var out agentcontext.Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || !out.Partial || len(out.Failures) != 1 || len(out.Sources) != 0 || fetched.Load() {
		t.Fatalf("private target protection: %+v fetched=%v", out, fetched.Load())
	}
}

func TestContextEndpointSharedDeadline(t *testing.T) {
	engine := &engineMock{name: "google", initialized: true, searchFn: func(ctx context.Context, _ Query) ([]SearchResult, error) { <-ctx.Done(); return nil, ctx.Err() }}
	opts := DefaultServerOptions()
	opts.MegaTimeout = 20 * time.Millisecond
	opts.RequestTimeout = time.Second
	s := NewServerWithOptions("127.0.0.1", 0, opts, engine)
	start := time.Now()
	resp := contextRequest(t, s, `{"query":"replication"}`)
	var out agentcontext.Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || !out.Partial || len(out.Failures) == 0 || time.Since(start) > time.Second {
		t.Fatalf("deadline result: %+v", out)
	}
}
