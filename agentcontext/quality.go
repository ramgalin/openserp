package agentcontext

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	inlineLink = regexp.MustCompile(`!?\[([^\]\n]*)\]\([^\n)]*\)`)
	bareURL    = regexp.MustCompile(`(?i)https?://[^\s<>]+`)
)

// Link destinations are provenance, not evidence. Keep visible labels for
// ranking; never rewrite the Markdown used by citation spans and hashes.
func visiblePassageText(text string) string {
	return bareURL.ReplaceAllString(inlineLink.ReplaceAllString(text, "$1"), "")
}

// Recognize narrow UI patterns rather than banning topic words such as
// "cookies", "subscribe", or "report", which also occur in technical prose.
func servicePassage(text string) bool {
	visible := strings.ToLower(strings.Join(strings.Fields(visiblePassageText(text)), " "))
	label := strings.Trim(visible, "#* _|:>\t\r\n")
	switch label {
	case "navigation", "table of contents", "on this page", "skip to content", "back to top", "home", "previous", "next", "sign in", "log in":
		return true
	}
	for _, prefix := range []string{
		"was this page helpful", "did you find this page helpful",
		"subscribe to our newsletter", "sign up for our newsletter", "join our newsletter",
	} {
		if strings.HasPrefix(label, prefix) {
			return true
		}
	}
	if strings.HasPrefix(label, "if you see anything in the documentation") &&
		strings.Contains(label, "report a documentation issue") {
		return true
	}
	if len(label) <= 600 && (strings.HasPrefix(label, "we use cookies") || strings.HasPrefix(label, "this website uses cookies") || strings.HasPrefix(label, "this site uses cookies")) &&
		(strings.Contains(label, "accept") || strings.Contains(label, "consent") || strings.Contains(label, "preferences")) {
		return true
	}
	if len(label) <= 200 && (strings.HasPrefix(label, "copyright") || strings.HasPrefix(label, "©")) && strings.Contains(label, "all rights reserved") {
		return true
	}
	// Link-only navigation may contain several links and separators. Do not
	// discard link lists with substantive labels, such as documentation topics.
	if inlineLink.MatchString(text) && strings.TrimFunc(inlineLink.ReplaceAllString(text, ""), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}) == "" {
		navigation := map[string]bool{"home": true, "previous": true, "prev": true, "next": true, "up": true, "top": true, "back": true, "to": true, "contents": true, "index": true}
		for word := range words(visible) {
			if !navigation[word] {
				return false
			}
		}
		return true
	}
	return false
}
