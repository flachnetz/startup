package boff

import (
	"regexp"
	"strings"

	"github.com/labstack/echo/v5"
)

// scopePathHeader carries the tenant scope prefix of the page, e.g.
// "/op/acme/all", set by the host that embeds the page. The scope lives in the
// URL path so every browser tab has its own, which means a link to another page
// of the host must start with it.
const scopePathHeader = "Fragment-Scope-Path"

// scopeSegment is one path segment of a scope prefix. Dot segments are
// rejected separately.
const scopeSegment = `[A-Za-z0-9._~%-]+`

var (
	scopePrefixRe = regexp.MustCompile(`^/op/` + scopeSegment + `/` + scopeSegment + `$`)

	// leadingScopeRe matches a scope prefix at the start of a path, whatever the
	// scope is.
	leadingScopeRe = regexp.MustCompile(`^/op/[^/?#]+/[^/?#]+`)
)

// scopePrefixOf returns the validated scope prefix of the request, or "" when
// the page is not embedded in a scope (opened directly, or by a host that does not send the header).
func scopePrefixOf(c *echo.Context) string {
	prefix := strings.TrimRight(strings.TrimSpace(c.Request().Header.Get(scopePathHeader)), "/")
	if !scopePrefixRe.MatchString(prefix) {
		return ""
	}

	for _, segment := range strings.Split(prefix, "/") {
		if segment == "." || segment == ".." {
			return ""
		}
	}

	return prefix
}

// ScopedPath puts the scope of the current request in front of an absolute path
// to a page of the host that is served by another service, e.g. "/players/view/1".
// A service's own links are built from Fragment-Base-Path, which already
// carries the scope. The path is returned unchanged when it is not absolute,
// already starts with a scope or the request has no scope.
func ScopedPath(c *echo.Context, path string) string {
	prefix := scopePrefixOf(c)
	if prefix == "" || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || leadingScopeRe.MatchString(path) {
		return path
	}

	return prefix + path
}

// unscopedPath removes a leading scope prefix. Cookies are shared by all tabs
// but every tab has its own scope, so paths are stored without it.
func unscopedPath(path string) string {
	loc := leadingScopeRe.FindStringIndex(path)
	if loc == nil {
		return path
	}

	rest := path[loc[1]:]
	if rest == "" || rest[0] == '?' || rest[0] == '#' {
		return "/" + rest
	}

	return rest
}
