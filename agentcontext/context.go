// Package agentcontext builds bounded, source-backed context without an LLM.
// Search and fetch are injected so local and hosted callers can retain their
// own routing, authentication, accounting, and network policies.
package agentcontext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/tiktoken-go/tokenizer"
)

type Request struct {
	Query      string   `json:"query"`
	Queries    []string `json:"queries,omitempty"`
	MaxSources int      `json:"max_sources,omitempty"`
	MaxFetches int      `json:"max_fetches,omitempty"`
	MaxTokens  int      `json:"max_tokens,omitempty"`
}

// Normalize validates all work limits before invoking any callback. Query is
// the ranking question; Queries contains optional additional search variants.
func (r *Request) Normalize() error {
	r.Query = strings.TrimSpace(r.Query)
	if r.Query == "" || len(r.Query) > 2000 || len(r.Queries) > 4 {
		return fmt.Errorf("query must contain 1..2000 bytes; at most 4 additional queries are allowed")
	}
	seen := map[string]bool{strings.ToLower(r.Query): true}
	queries := []string{}
	for _, q := range r.Queries {
		q = strings.TrimSpace(q)
		if q == "" || len(q) > 2000 {
			return fmt.Errorf("each additional query must contain 1..2000 bytes")
		}
		key := strings.ToLower(q)
		if !seen[key] {
			queries = append(queries, q)
			seen[key] = true
		}
	}
	r.Queries = queries
	if r.MaxSources == 0 {
		r.MaxSources = 5
	}
	if r.MaxFetches == 0 {
		r.MaxFetches = max(8, r.MaxSources)
	}
	if r.MaxTokens == 0 {
		r.MaxTokens = 6000
	}
	if r.MaxSources < 1 || r.MaxSources > 10 || r.MaxFetches < r.MaxSources || r.MaxFetches > 20 || r.MaxTokens < 256 || r.MaxTokens > 32000 {
		return fmt.Errorf("max_sources must be 1..10, max_fetches max_sources..20, max_tokens 256..32000")
	}
	return nil
}

type Hit struct {
	URL, Title, Engine string
	Rank               int
}

type Failure struct {
	Stage string `json:"stage"`
	Query string `json:"query,omitempty"`
	URL   string `json:"url,omitempty"`
	Code  string `json:"code"`
}

type SearchResult struct {
	Hits     []Hit
	Failures []Failure
}

// Document.Markdown is the exact UTF-8 string addressed by citation byte spans.
type Document struct{ Markdown, Title, FetchedAt string }
type SearchFunc func(context.Context, string) (SearchResult, error)
type FetchFunc func(context.Context, string) (Document, error)

type Span struct {
	Start int `json:"start"`
	End   int `json:"end"`
}
type Citation struct {
	ID         string  `json:"id"`
	SourceID   string  `json:"source_id"`
	Excerpt    string  `json:"excerpt"`
	SourceSpan Span    `json:"source_span"`
	Score      float64 `json:"score"`
	Truncated  bool    `json:"truncated"`
}
type Source struct {
	ID            string `json:"id"`
	URL           string `json:"url"`
	Title         string `json:"title"`
	FetchedAt     string `json:"fetched_at"`
	ContentSHA256 string `json:"content_sha256"`
}
type Response struct {
	Query     string     `json:"query"`
	Queries   []string   `json:"queries"`
	Context   string     `json:"context"`
	Sources   []Source   `json:"sources"`
	Citations []Citation `json:"citations"`
	Failures  []Failure  `json:"failures"`
	Partial   bool       `json:"partial"`
	Truncated bool       `json:"truncated"`
	Usage     Usage      `json:"usage"`
}
type Usage struct {
	Tokenizer     string `json:"tokenizer"`
	ContextTokens int    `json:"context_tokens"`
	MaxTokens     int    `json:"max_tokens"`
	Searches      int    `json:"searches"`
	Fetches       int    `json:"fetches"`
}

type candidate struct {
	Hit
	score float64
}
type passage struct {
	source int
	span   Span
	score  float64
}

// Build limits query fanout to three concurrent searches and fetch fanout to
// two. All callbacks must honor ctx and impose their own response-size limits.
// max_tokens applies to Context, including citation headers, not the JSON envelope.
func Build(ctx context.Context, req Request, search SearchFunc, fetch FetchFunc) (*Response, error) {
	if err := req.Normalize(); err != nil {
		return nil, err
	}
	enc, err := tokenizer.Get(tokenizer.Cl100kBase)
	if err != nil {
		return nil, err
	}
	queries := append([]string{req.Query}, req.Queries...)
	out := &Response{Query: req.Query, Queries: queries, Sources: []Source{}, Citations: []Citation{}, Failures: []Failure{}, Usage: Usage{Tokenizer: "cl100k_base", MaxTokens: req.MaxTokens}}
	batches := make([]SearchResult, len(queries))
	// Each goroutine owns one output slot; joining before merging also makes
	// ties deterministic regardless of network completion order.
	var wg sync.WaitGroup
	var searches, fetches atomic.Int32
	sem := make(chan struct{}, 3)
	for i, q := range queries {
		wg.Add(1)
		go func(i int, q string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				batches[i].Failures = []Failure{{Stage: "search", Query: q, Code: "deadline_exceeded"}}
				return
			}
			defer func() { <-sem }()
			if ctx.Err() != nil {
				batches[i].Failures = []Failure{{Stage: "search", Query: q, Code: "deadline_exceeded"}}
				return
			}
			searches.Add(1)
			r, e := search(ctx, q)
			if e != nil {
				r.Failures = append(r.Failures, Failure{Stage: "search", Query: q, Code: "search_failed"})
			}
			batches[i] = r
		}(i, q)
	}
	wg.Wait()
	out.Usage.Searches = int(searches.Load())
	byURL := map[string]*candidate{}
	for _, batch := range batches {
		out.Failures = append(out.Failures, batch.Failures...)
		seen := map[string]bool{}
		for _, hit := range batch.Hits {
			key := normalizeURL(hit.URL)
			if key == "" {
				continue
			}
			occurrence := key + "\x00" + hit.Engine
			if seen[occurrence] {
				continue
			}
			seen[occurrence] = true
			if hit.Rank < 1 {
				hit.Rank = 1
			}
			c := byURL[key]
			if c == nil {
				c = &candidate{Hit: hit}
				byURL[key] = c
			}
			c.score += 1 / float64(60+hit.Rank)
		}
	}
	candidates := make([]candidate, 0, len(byURL))
	for _, c := range byURL {
		candidates = append(candidates, *c)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].URL < candidates[j].URL
	})
	if len(candidates) > req.MaxFetches {
		candidates = candidates[:req.MaxFetches]
	}
	docs := make([]Document, len(candidates))
	fetchErrors := make([]bool, len(candidates))
	sem = make(chan struct{}, 2)
	for i, c := range candidates {
		wg.Add(1)
		go func(i int, c candidate) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				fetchErrors[i] = true
				return
			}
			defer func() { <-sem }()
			if ctx.Err() != nil {
				fetchErrors[i] = true
				return
			}
			fetches.Add(1)
			doc, e := fetch(ctx, c.URL)
			if e != nil || len(doc.Markdown) > 2*1024*1024 || strings.TrimSpace(doc.Markdown) == "" {
				fetchErrors[i] = true
				return
			}
			docs[i] = doc
		}(i, c)
	}
	wg.Wait()
	out.Usage.Fetches = int(fetches.Load())
	terms := words(req.Query)
	passages := []passage{}
	for i, doc := range docs {
		if fetchErrors[i] {
			out.Failures = append(out.Failures, Failure{Stage: "fetch", URL: candidates[i].URL, Code: "fetch_failed"})
			continue
		}
		for _, span := range splitPassages(doc.Markdown) {
			textTerms := words(visiblePassageText(doc.Markdown[span.Start:span.End]))
			matched := 0
			for term := range terms {
				if textTerms[term] {
					matched++
				}
			}
			coverage := 0.0
			if len(terms) > 0 {
				coverage = float64(matched) / float64(len(terms))
			}
			passages = append(passages, passage{source: i, span: span, score: coverage + candidates[i].score})
		}
	}
	sort.SliceStable(passages, func(i, j int) bool { return passages[i].score > passages[j].score })
	// First offer each source its best passage, then allow up to three per
	// source. This keeps a long page from consuming the whole evidence budget.
	ordered := []passage{}
	seenSources := map[int]bool{}
	for _, p := range passages {
		if !seenSources[p.source] {
			ordered = append(ordered, p)
			seenSources[p.source] = true
		}
	}
	seenSources = map[int]bool{}
	for _, p := range passages {
		if !seenSources[p.source] {
			seenSources[p.source] = true
			continue
		}
		ordered = append(ordered, p)
	}
	counts := map[int]int{}
	sourceIDs := map[int]string{}
	for _, p := range ordered {
		if counts[p.source] >= 3 {
			continue
		}
		sid := sourceIDs[p.source]
		if sid == "" && len(out.Sources) >= req.MaxSources {
			continue
		}
		if sid == "" {
			sid = fmt.Sprintf("s%d", len(out.Sources)+1)
		}
		doc, c := docs[p.source], candidates[p.source]
		id := fmt.Sprintf("c%d", len(out.Citations)+1)
		title := doc.Title
		if title == "" {
			title = c.Title
		}
		if runes := []rune(title); len(runes) > 512 {
			title = string(runes[:512])
		}
		// Headers are single-line and inside the same measured budget.
		header := fmt.Sprintf("[%s] %s\n%s\n", id, strings.Join(strings.Fields(title), " "), c.URL)
		prefix := ""
		if out.Context != "" {
			prefix = "\n\n"
		}
		excerpt := doc.Markdown[p.span.Start:p.span.End]
		block := prefix + header + excerpt
		n, e := enc.Count(out.Context + block)
		if e != nil {
			return nil, e
		}
		clipped := false
		if n > req.MaxTokens {
			clipped = true
			// Binary search a UTF-8-safe prefix; final recount makes the bound
			// exact even where BPE merges change at the cut boundary.
			runes := []rune(excerpt)
			lo, hi := 0, len(runes)
			for lo < hi {
				mid := (lo + hi + 1) / 2
				size, e := enc.Count(out.Context + prefix + header + string(runes[:mid]))
				if e != nil {
					return nil, e
				}
				if size <= req.MaxTokens {
					lo = mid
				} else {
					hi = mid - 1
				}
			}
			excerpt = strings.TrimRightFunc(string(runes[:lo]), unicode.IsSpace)
			block = prefix + header + excerpt
			n, e = enc.Count(out.Context + block)
			if e != nil {
				return nil, e
			}
			if excerpt == "" || n > req.MaxTokens {
				out.Truncated = true
				continue
			}
		}
		out.Context += block
		out.Usage.ContextTokens = n
		if sourceIDs[p.source] == "" {
			hash := sha256.Sum256([]byte(doc.Markdown))
			out.Sources = append(out.Sources, Source{ID: sid, URL: c.URL, Title: title, FetchedAt: doc.FetchedAt, ContentSHA256: hex.EncodeToString(hash[:])})
			sourceIDs[p.source] = sid
		}
		out.Citations = append(out.Citations, Citation{ID: id, SourceID: sid, Excerpt: excerpt, SourceSpan: Span{Start: p.span.Start, End: p.span.Start + len(excerpt)}, Score: p.score, Truncated: clipped})
		counts[p.source]++
		out.Truncated = out.Truncated || clipped
	}
	out.Partial = len(out.Failures) > 0 || len(out.Citations) == 0
	return out, nil
}

func normalizeURL(raw string) string {
	if len(raw) > 8192 {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return ""
	}
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	q := u.Query()
	for key := range q {
		if strings.HasPrefix(strings.ToLower(key), "utm_") || key == "gclid" || key == "fbclid" {
			q.Del(key)
		}
	}
	u.RawQuery = q.Encode()
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String()
}

func words(text string) map[string]bool {
	out := map[string]bool{}
	for _, w := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) }) {
		if len([]rune(w)) > 1 {
			out[w] = true
		}
	}
	return out
}

// Split at paragraph boundaries, then at most 800 runes. Every span refers to
// the untouched input, including when non-ASCII text precedes the passage.
func splitPassages(text string) []Span {
	spans := []Span{}
	for start := 0; start < len(text); {
		end := len(text)
		if n := strings.Index(text[start:], "\n\n"); n >= 0 {
			end = start + n
		}
		// Filter complete paragraphs before chunking so a long feedback or
		// consent block cannot leave an apparently useful tail. Offsets still
		// refer to the original, unmodified document.
		if servicePassage(text[start:end]) {
			start = end + 2
			continue
		}
		partStart := start
		count := 0
		for offset := range text[start:end] {
			if count == 800 {
				spans = appendTrimmed(spans, text, partStart, start+offset)
				partStart = start + offset
				count = 0
			}
			count++
		}
		spans = appendTrimmed(spans, text, partStart, end)
		start = end + 2
	}
	return spans
}

func appendTrimmed(spans []Span, text string, start, end int) []Span {
	part := text[start:end]
	left := strings.TrimLeftFunc(part, unicode.IsSpace)
	start += len(part) - len(left)
	end = start + len(strings.TrimRightFunc(left, unicode.IsSpace))
	if start < end {
		spans = append(spans, Span{start, end})
	}
	return spans
}
