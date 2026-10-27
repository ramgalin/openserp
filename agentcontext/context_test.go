package agentcontext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tiktoken-go/tokenizer"
)

func TestBuildRelevantPassagesWithExactProvenance(t *testing.T) {
	markdown := "Навигация и новости сайта.\n\n" + strings.Repeat("Unrelated introduction. ", 100) + "\n\nLogical replication restrictions include unsupported DDL and sequences.\n\nOther information."
	search := func(context.Context, string) (SearchResult, error) {
		return SearchResult{Hits: []Hit{{URL: "https://example.com/docs", Engine: "google", Rank: 1}}}, nil
	}
	fetch := func(context.Context, string) (Document, error) {
		return Document{Markdown: markdown, Title: "Replication", FetchedAt: "2026-10-24T00:00:00Z"}, nil
	}
	out, err := Build(context.Background(), Request{Query: "logical replication restrictions", MaxTokens: 256}, search, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Citations) == 0 || !strings.Contains(out.Citations[0].Excerpt, "unsupported DDL") {
		t.Fatalf("missed relevant later passage: %+v", out)
	}
	if len(out.Sources) != 1 || out.Sources[0].FetchedAt == "" {
		t.Fatalf("sources: %+v", out.Sources)
	}
	hash := sha256.Sum256([]byte(markdown))
	if out.Sources[0].ContentSHA256 != hex.EncodeToString(hash[:]) {
		t.Fatal("incorrect document hash")
	}
	for _, c := range out.Citations {
		if markdown[c.SourceSpan.Start:c.SourceSpan.End] != c.Excerpt {
			t.Fatalf("incorrect UTF-8 span: %+v", c)
		}
		if !strings.Contains(out.Context, "["+c.ID+"]") || c.SourceID != out.Sources[0].ID {
			t.Fatal("broken citation link")
		}
	}
	assertBudget(t, out)
}

func TestBuildDeduplicatesVariantsAndURLTracking(t *testing.T) {
	var calls atomic.Int32
	search := func(context.Context, string) (SearchResult, error) {
		return SearchResult{Hits: []Hit{
			{URL: "https://example.com/docs?utm_source=one#part", Engine: "google", Rank: 1},
			{URL: "https://example.com/docs", Engine: "bing", Rank: 2},
			{URL: "https://example.com/docs#other", Engine: "google", Rank: 3},
		}}, nil
	}
	fetch := func(context.Context, string) (Document, error) {
		calls.Add(1)
		return Document{Markdown: "Database replication documentation."}, nil
	}
	out, err := Build(context.Background(), Request{Query: "replication", Queries: []string{"Replication", "database replication", "database replication"}}, search, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || out.Usage.Searches != 2 || out.Usage.Fetches != 1 {
		t.Fatalf("unexpected work: %+v", out.Usage)
	}
	if len(out.Sources) != 1 || out.Partial {
		t.Fatalf("unexpected result: %+v", out)
	}
}

func TestBuildKeepsUsableEvidenceOnFailures(t *testing.T) {
	search := func(_ context.Context, q string) (SearchResult, error) {
		if q == "failed variant" {
			return SearchResult{}, errors.New("secret provider error")
		}
		return SearchResult{Hits: []Hit{{URL: "https://example.com/bad", Rank: 1}, {URL: "https://example.com/good", Rank: 2}}, Failures: []Failure{{Stage: "search", Code: "engine_unavailable"}}}, nil
	}
	fetch := func(_ context.Context, u string) (Document, error) {
		if strings.HasSuffix(u, "/bad") {
			return Document{}, errors.New("proxy password")
		}
		return Document{Markdown: "Relevant replication evidence."}, nil
	}
	out, err := Build(context.Background(), Request{Query: "replication", Queries: []string{"failed variant"}, MaxSources: 1, MaxFetches: 2}, search, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Partial || len(out.Failures) != 3 || len(out.Sources) != 1 || !strings.HasSuffix(out.Sources[0].URL, "/good") {
		t.Fatalf("unexpected partial response: %+v", out)
	}
	if strings.Contains(fmt.Sprint(out), "password") || strings.Contains(fmt.Sprint(out), "secret provider") {
		t.Fatal("leaked callback errors")
	}
}

func TestBuildStrictBudgetAcrossLanguages(t *testing.T) {
	for _, text := range []string{strings.Repeat("репликация ограничение последовательность ", 100), strings.Repeat("数据库复制限制😀 ", 200), strings.Repeat("logical replication limitations 1234567890 abcdefghijklmnopqrstuvwxyz ", 200)} {
		t.Run(text[:8], func(t *testing.T) {
			search := func(context.Context, string) (SearchResult, error) {
				return SearchResult{Hits: []Hit{{URL: "https://example.com/", Rank: 1}}}, nil
			}
			fetch := func(context.Context, string) (Document, error) { return Document{Markdown: text}, nil }
			out, err := Build(context.Background(), Request{Query: "replication", MaxTokens: 256}, search, fetch)
			if err != nil {
				t.Fatal(err)
			}
			assertBudget(t, out)
			if !out.Truncated || len(out.Citations) == 0 || !utf8.ValidString(out.Context) {
				t.Fatalf("invalid truncation: %+v", out)
			}
			for _, c := range out.Citations {
				if text[c.SourceSpan.Start:c.SourceSpan.End] != c.Excerpt {
					t.Fatal("truncated span mismatch")
				}
			}
		})
	}
}

func TestBuildRejectsInvalidLimitsBeforeWork(t *testing.T) {
	for _, r := range []Request{{}, {Query: "x", MaxTokens: 1}, {Query: "x", MaxSources: 11}, {Query: "x", MaxSources: 4, MaxFetches: 2}, {Query: "x", MaxFetches: 21}, {Query: "x", Queries: []string{""}}, {Query: "x", Queries: []string{"a", "b", "c", "d", "e"}}} {
		_, err := Build(context.Background(), r, func(context.Context, string) (SearchResult, error) {
			t.Fatal("search on invalid request")
			return SearchResult{}, nil
		}, nil)
		if err == nil {
			t.Fatalf("accepted %+v", r)
		}
	}
}

func TestBuildCancellationAndWorkCaps(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := Build(ctx, Request{Query: "replication"}, func(context.Context, string) (SearchResult, error) {
		t.Fatal("called after cancel")
		return SearchResult{}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Partial || out.Usage.Searches != 0 || len(out.Failures) == 0 {
		t.Fatalf("cancellation not reported: %+v", out)
	}
	var calls, active, peak atomic.Int32
	search := func(context.Context, string) (SearchResult, error) {
		r := SearchResult{}
		for i := 0; i < 30; i++ {
			r.Hits = append(r.Hits, Hit{URL: fmt.Sprintf("https://example.com/%02d", i), Rank: i + 1})
		}
		return r, nil
	}
	fetch := func(context.Context, string) (Document, error) {
		calls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		return Document{Markdown: "Replication limits."}, nil
	}
	out, err = Build(context.Background(), Request{Query: "replication", MaxSources: 2, MaxFetches: 4}, search, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 || peak.Load() > 2 || len(out.Sources) != 2 {
		t.Fatalf("work caps: calls=%d peak=%d sources=%d", calls.Load(), peak.Load(), len(out.Sources))
	}
}

func assertBudget(t *testing.T, out *Response) {
	t.Helper()
	enc, err := tokenizer.Get(tokenizer.Cl100kBase)
	if err != nil {
		t.Fatal(err)
	}
	n, err := enc.Count(out.Context)
	if err != nil {
		t.Fatal(err)
	}
	if n != out.Usage.ContextTokens || n > out.Usage.MaxTokens {
		t.Fatalf("budget: actual=%d reported=%d max=%d", n, out.Usage.ContextTokens, out.Usage.MaxTokens)
	}
}
