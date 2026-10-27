package agentcontext

import (
	"context"
	"os"
	"strings"
	"testing"
)

func buildMarkdown(t *testing.T, query, markdown string, budget int) *Response {
	t.Helper()
	out, err := Build(context.Background(), Request{Query: query, MaxSources: 1, MaxFetches: 1, MaxTokens: budget},
		func(context.Context, string) (SearchResult, error) {
			return SearchResult{Hits: []Hit{{URL: "https://www.postgresql.org/docs/current/logical-replication-restrictions.html", Title: "29.8. Restrictions", Rank: 1}}}, nil
		}, func(context.Context, string) (Document, error) { return Document{Markdown: markdown}, nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range out.Citations {
		if markdown[c.SourceSpan.Start:c.SourceSpan.End] != c.Excerpt {
			t.Fatal("citation no longer addresses original Markdown")
		}
	}
	assertBudget(t, out)
	return out
}

func TestPostgresRestrictionsExcludesFeedback(t *testing.T) {
	data, err := os.ReadFile("testdata/postgres-restrictions.md")
	if err != nil {
		t.Fatal(err)
	}
	out := buildMarkdown(t, "PostgreSQL logical replication restrictions", string(data), 512)
	if len(out.Citations) == 0 {
		t.Fatal("no useful evidence")
	}
	if strings.Contains(out.Context, "report a documentation issue") || strings.Contains(out.Context, "please use [this form]") {
		t.Fatalf("feedback in context: %s", out.Context)
	}
	if !strings.Contains(out.Context, "DDL commands are not replicated") {
		t.Fatalf("lost substantive restriction: %s", out.Context)
	}
	t.Logf("tokens=%d citations=%d\n%s", out.Usage.ContextTokens, len(out.Citations), out.Context)
}

func TestLinkDestinationsDoNotBoostRanking(t *testing.T) {
	markdown := "Русский префикс.\n\nFor assistance, visit [support](https://example.com/logical-replication-restrictions).\n\nLogical replication restrictions include unsupported schema changes."
	out := buildMarkdown(t, "logical replication restrictions", markdown, 256)
	if !strings.HasPrefix(out.Citations[0].Excerpt, "Logical replication restrictions") {
		t.Fatalf("ranked URL instead of evidence: %+v", out.Citations)
	}
}

func TestBoilerplateCannotFillEvidenceBudget(t *testing.T) {
	markdown := "# Navigation\n\n[Home](https://example.com) | [Previous](https://example.com/prev) | [Next](https://example.com/next)\n\nWas this page helpful? [Yes](https://example.com/yes) [No](https://example.com/no)\n\nWe use cookies to improve your experience. Accept all cookies.\n\nSubscribe to our newsletter for replication updates.\n\nCopyright 2026 Example. All rights reserved."
	out := buildMarkdown(t, "replication", markdown, 512)
	if len(out.Citations) != 0 || len(out.Sources) != 0 || out.Context != "" || !out.Partial {
		t.Fatalf("boilerplate-only page became evidence: %+v", out)
	}
}

func TestTechnicalCookieAndSubscriptionContentSurvives(t *testing.T) {
	markdown := "To subscribe to logical replication, create a subscription on the subscriber.\n\nCookie restrictions prevent third-party authentication cookies from being sent.\n\nTo report a documentation issue, use the issue endpoint in the API: this paragraph explains its implementation."
	out := buildMarkdown(t, "replication cookie restrictions", markdown, 512)
	if len(out.Citations) != 3 {
		t.Fatalf("removed substantive technical content: %+v", out.Citations)
	}
}

func TestLongServiceBlockIsFilteredBeforeChunking(t *testing.T) {
	markdown := "Was this page helpful? " + strings.Repeat("Send replication documentation feedback. ", 100) +
		"\n\nLogical replication does not replicate sequence data."
	out := buildMarkdown(t, "logical replication", markdown, 512)
	if len(out.Citations) != 1 || !strings.HasPrefix(out.Citations[0].Excerpt, "Logical replication") {
		t.Fatalf("service block tail leaked through chunking: %+v", out.Citations)
	}
}

func TestUsefulMarkdownLinkLabelsRemainEvidence(t *testing.T) {
	markdown := "See [logical replication restrictions](https://example.com/docs).\n\n[Schema changes](https://example.com/schema) | [Sequence data](https://example.com/sequences)"
	out := buildMarkdown(t, "logical replication restrictions", markdown, 512)
	if len(out.Citations) != 2 || !strings.HasPrefix(out.Citations[0].Excerpt, "See [logical replication restrictions]") {
		t.Fatalf("lost visible link labels or substantive link list: %+v", out.Citations)
	}
}
