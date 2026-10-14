package boff

import (
	"encoding/base64"
	"encoding/json"
	"html/template"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

// breadcrumbCookieName is the name of the cookie used to carry the breadcrumb
// navigation trail between backoffice pages, potentially served by
// different backends on the same domain.
const breadcrumbCookieName = "BackofficeBreadcrumbsV1"

// navigationCookieName is the name of the cookie used to carry the persistent
// top-level navigation links between backoffice pages, potentially served by
// different backends on the same domain.
const navigationCookieName = "BackofficeNavV1"

// Breadcrumb is a single entry in the cross-service navigation trail.
//
// Key is a stable logical identifier for the resource shown on the page,
// e.g. "player:abcdef" or "order:123". It is used to look up an entry
// across services, independent of the actual URL structure.
//
// Label and Path are only known to (and only ever set by) the service that
// owns the resource identified by Key. Other services may reference the
// same Key without knowing Label or Path.
type Breadcrumb struct {
	Key   string `json:"k"`
	Label string `json:"l"`
	Path  string `json:"p"`
}

// BreadcrumbRemote creates a Breadcrumb for an ancestor resource whose label and
// path are not known to the calling service. It will be rendered without a
// link unless a cached entry with the same Key is already known.
func BreadcrumbRemote(key string) Breadcrumb {
	return Breadcrumb{Key: key}
}

// BreadcrumbRemoteWithLabel creates a Breadcrumb for an ancestor resource whose
// label is known to the calling service, but whose path is not. It will be
// rendered without a link unless a cached entry with the same Key is
// already known. BreadcrumbRemote should be preferred.
func BreadcrumbRemoteWithLabel(key, label string) Breadcrumb {
	return Breadcrumb{Key: key, Label: label}
}

// BreadcrumbLocal creates a Breadcrumb for a resource owned by the calling
// service, with an authoritative label and path.
func BreadcrumbLocal(key, label, path string) Breadcrumb {
	return Breadcrumb{Key: key, Label: label, Path: path}
}

// NavLink is a persistent, top-level navigation link that, unlike a
// Breadcrumb, is not part of the current drill-down trail. It is stored
// alongside the breadcrumb trail so it survives across services too.
type NavLink struct {
	Label        string `json:"l"`
	Path         string `json:"p"`
	RequiredRole Role   `json:"r"`

	// IsActive is set by LoadNavigation when Path is a prefix of the
	// current request path. It is never persisted in the cookie.
	IsActive bool `json:"-"`
}

const (
	// maxBreadcrumbCache is the number of ancestor entries (label and path by
	// key) the breadcrumb cookie remembers.
	maxBreadcrumbCache = 30

	// maxNavigationSets is the number of navigation link sets, one per subject
	// (e.g. one per player), the navigation cookie remembers.
	maxNavigationSets = 6

	// maxCookieValueLen is the largest encoded cookie value we write. Browsers
	// drop a cookie above about 4096 bytes for name, value and attributes, so the
	// oldest entries are evicted before that is reached.
	maxCookieValueLen = 3800

	// breadcrumbKeysContextKey holds the keys of the breadcrumb chain of the
	// current request, deepest first, see ReconcileBreadcrumbs.
	breadcrumbKeysContextKey = "boff.breadcrumb-keys"
)

// breadcrumbCookie is the JSON-serialized content stored in the breadcrumb
// cookie.
//
// The cookie is shared by all browser tabs, so it must not describe one
// trail: it is a cache of label and path by key, which any tab can read to
// fill in the ancestors another service owns.
type breadcrumbCookie struct {
	// Breadcrumbs is only read, to pick up a cookie written before Cache existed.
	Breadcrumbs []Breadcrumb `json:"e,omitempty"`

	// Cache holds the most recently used entries first.
	Cache []Breadcrumb `json:"c,omitempty"`
}

// navigationCookie is the JSON-serialized content stored in the navigation
// cookie.
type navigationCookie struct {
	// Sets holds the most recently stored set first.
	Sets []navSet `json:"s,omitempty"`
}

// navSet is the navigation of one subject, identified by the key of the
// deepest breadcrumb of the page that stored it (e.g. "player:123"). Tabs on
// different subjects each find their own links.
type navSet struct {
	Key   string    `json:"k"`
	Links []NavLink `json:"l"`
}

// Reconcile merges the given breadcrumb chain with the entries already
// known from the cookie and returns the resulting BreadcrumbCookie to
// persist.
//
// new represents the full logical chain for the page currently being
// rendered, ordered from root to leaf. Only the last entry (the current
// page) is expected to carry an authoritative Label and Path. For every
// earlier entry, Reconcile starts from the existing cookie entry with the
// same Key (if any) and only overwrites its Label/Path with the values
// from new when those are non-empty. If no cached entry exists yet, the
// entry from new is kept as-is (typically with an empty Path, meaning it
// cannot be rendered as a link).
func (c breadcrumbCookie) Reconcile(new []Breadcrumb) breadcrumbCookie {
	known := make(map[string]Breadcrumb, len(c.Cache)+len(c.Breadcrumbs))
	for _, entry := range append(slices.Clone(c.Breadcrumbs), c.Cache...) {
		known[entry.Key] = entry
	}

	merged := make([]Breadcrumb, len(new))
	for i, entry := range new {
		if i == len(new)-1 {
			// The current page is always authoritative for its own entry.
			merged[i] = entry
			continue
		}

		if cached, ok := known[entry.Key]; ok {
			if entry.Label != "" {
				cached.Label = entry.Label
			}

			if entry.Path != "" {
				cached.Path = entry.Path
			}

			merged[i] = cached
		} else {
			merged[i] = entry
		}
	}

	return breadcrumbCookie{Breadcrumbs: merged}
}

// ReconcileBreadcrumbs loads the current breadcrumb cookie, reconciles it
// with the given breadcrumb chain (see BreadcrumbCookie.Reconcile), adds the
// chain to the cookie's cache, and returns the reconciled breadcrumbs.
//
// It also records the keys of the chain on c. UpdateNavigation and
// LoadNavigation use them to find the navigation of this page's subject, so
// call it before them.
func ReconcileBreadcrumbs(c *echo.Context, entries ...Breadcrumb) []Breadcrumb {
	keys := make([]string, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Key != "" {
			keys = append(keys, entries[i].Key)
		}
	}

	c.Set(breadcrumbKeysContextKey, keys)

	cookie, _ := loadBreadcrumbCookie(c)
	for i := range cookie.Breadcrumbs {
		cookie.Breadcrumbs[i].Path = unscopedPath(cookie.Breadcrumbs[i].Path)
	}

	for i := range cookie.Cache {
		cookie.Cache[i].Path = unscopedPath(cookie.Cache[i].Path)
	}

	stripped := make([]Breadcrumb, len(entries))
	for i, entry := range entries {
		entry.Path = unscopedPath(entry.Path)
		stripped[i] = entry
	}

	reconciled := cookie.Reconcile(stripped)
	cookie.remember(reconciled.Breadcrumbs).Save(c)

	// The cookie is shared by all tabs and holds no scope; the links on the page
	// get the scope of this request.
	for i := range reconciled.Breadcrumbs {
		reconciled.Breadcrumbs[i].Path = ScopedPath(c, reconciled.Breadcrumbs[i].Path)
	}

	return reconciled.Breadcrumbs
}

// remember returns the cookie to store after rendering chain: the chain first,
// then what was cached before, without entries that carry nothing to show.
func (bc breadcrumbCookie) remember(chain []Breadcrumb) breadcrumbCookie {
	seen := make(map[string]bool, maxBreadcrumbCache)
	cache := make([]Breadcrumb, 0, maxBreadcrumbCache)

	for _, group := range [][]Breadcrumb{chain, bc.Cache, bc.Breadcrumbs} {
		for _, entry := range group {
			if entry.Key == "" || (entry.Label == "" && entry.Path == "") || seen[entry.Key] {
				continue
			}

			seen[entry.Key] = true
			cache = append(cache, entry)
		}
	}

	return breadcrumbCookie{Cache: cache[:min(len(cache), maxBreadcrumbCache)]}
}

// loadBreadcrumbCookie reads and decodes the breadcrumb cookie from the
// request. It returns false if the cookie is missing or could not be
// decoded, in which case the caller should treat the trail as empty.
func loadBreadcrumbCookie(c *echo.Context) (breadcrumbCookie, bool) {
	cookie, err := c.Cookie(breadcrumbCookieName)
	if err != nil {
		return breadcrumbCookie{}, false
	}

	raw, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		return breadcrumbCookie{}, false
	}

	var bc breadcrumbCookie
	if err := json.Unmarshal(raw, &bc); err != nil {
		return breadcrumbCookie{}, false
	}

	return bc, true
}

// Save encodes the BreadcrumbCookie and writes it as the breadcrumb cookie
// on the response.
func (bc breadcrumbCookie) Save(c *echo.Context) {
	bc.Breadcrumbs = nil

	value := encodeCookieValue(bc)
	for len(value) > maxCookieValueLen && len(bc.Cache) > 0 {
		bc.Cache = bc.Cache[:len(bc.Cache)-1]
		value = encodeCookieValue(bc)
	}

	setCookie(c, breadcrumbCookieName, value)
}

func encodeCookieValue(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		// Only plain strings are marshaled, so this can't realistically fail.
		panic(err)
	}

	return base64.RawURLEncoding.EncodeToString(raw)
}

func setCookie(c *echo.Context, name, value string) {
	c.SetCookie(&http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
}

// loadNavigationCookie reads and decodes the navigation cookie from the
// request. It returns false if the cookie is missing or could not be
// decoded, in which case the caller should treat the link list as empty.
//
// Each NavLink's IsActive field is set to true if its Path is a prefix of
// the current request path.
func loadNavigationCookie(c *echo.Context) (navigationCookie, bool) {
	cookie, err := c.Cookie(navigationCookieName)
	if err != nil {
		return navigationCookie{}, false
	}

	raw, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		return navigationCookie{}, false
	}

	var nc navigationCookie
	if err := json.Unmarshal(raw, &nc); err != nil {
		return navigationCookie{}, false
	}

	for i := range nc.Sets {
		for j := range nc.Sets[i].Links {
			nc.Sets[i].Links[j].Path = unscopedPath(nc.Sets[i].Links[j].Path)
		}
	}

	return nc, true
}

func updateNavigationIsActive(c *echo.Context, nav []NavLink) {
	// Both sides are compared without scope: the stored links have none and
	// Real-Path carries the one of this tab.
	currentPath := path.Clean(unscopedPath(getRealPathFromContext(c)))
	for i := range nav {
		if nav[i].Path == "" {
			continue
		}

		nav[i].IsActive = strings.HasPrefix(currentPath, path.Clean(unscopedPath(nav[i].Path)))
	}
}

func getRealPathFromContext(c *echo.Context) string {
	if path := c.Request().Header.Get("Real-Path"); path != "" {
		return path
	}

	return c.Request().URL.Path
}

// navigationKeys are the breadcrumb keys of the current request, deepest
// first. They are empty when ReconcileBreadcrumbs has not run yet.
func navigationKeys(c *echo.Context) []string {
	keys, _ := c.Get(breadcrumbKeysContextKey).([]string)
	return keys
}

// LoadNavigation returns the navigation links of the subject of the current
// page: the set stored under the deepest breadcrumb key of this request that
// has one. The cookie is shared by all tabs but each tab finds the links of
// its own subject. It returns nil if there is none.
//
// Without breadcrumb keys (ReconcileBreadcrumbs has not been called for this
// request) the set stored last is returned.
func LoadNavigation(c *echo.Context) []NavLink {
	cookie, _ := loadNavigationCookie(c)

	var links []NavLink

	if keys := navigationKeys(c); len(keys) == 0 {
		if len(cookie.Sets) > 0 {
			links = cookie.Sets[0].Links
		}
	} else {
	search:
		for _, key := range keys {
			for _, set := range cookie.Sets {
				if set.Key == key {
					links = set.Links
					break search
				}
			}
		}
	}

	updateNavigationIsActive(c, links)
	scopeNavigation(c, links)

	return links
}

// Save encodes the navigationCookie and writes it as the navigation cookie
// on the response, dropping the oldest sets until it is small enough.
func (nc navigationCookie) Save(c *echo.Context) {
	value := encodeCookieValue(nc)
	for len(value) > maxCookieValueLen && len(nc.Sets) > 0 {
		nc.Sets = nc.Sets[:len(nc.Sets)-1]
		value = encodeCookieValue(nc)
	}

	setCookie(c, navigationCookieName, value)
}

// UpdateNavigation stores links as the navigation of the current page's
// subject, replacing the set stored for it before (empty links remove it).
// The subject is the deepest breadcrumb key of this request, so call
// ReconcileBreadcrumbs first. Sets of other subjects, e.g. open in another tab,
// are kept; the oldest ones are evicted.
//
// The cookie is shared by all tabs, so the links are stored without their
// scope. links itself is updated for rendering: IsActive is set and every
// path gets the scope of this request.
func UpdateNavigation(c *echo.Context, links []NavLink) {
	var key string
	if keys := navigationKeys(c); len(keys) > 0 {
		key = keys[0]
	}

	cookie, _ := loadNavigationCookie(c)

	sets := make([]navSet, 0, maxNavigationSets)

	if len(links) > 0 {
		stored := make([]NavLink, len(links))
		for i, link := range links {
			link.Path = unscopedPath(link.Path)
			stored[i] = link
		}

		sets = append(sets, navSet{Key: key, Links: stored})
	}

	for _, set := range cookie.Sets {
		if set.Key != key {
			sets = append(sets, set)
		}
	}

	navigationCookie{Sets: sets[:min(len(sets), maxNavigationSets)]}.Save(c)

	updateNavigationIsActive(c, links)
	scopeNavigation(c, links)
}

// scopeNavigation puts the scope of the request in front of every link path.
func scopeNavigation(c *echo.Context, links []NavLink) {
	for i := range links {
		links[i].Path = ScopedPath(c, unscopedPath(links[i].Path))
	}
}

// BreadcrumbsBlock renders the breadcrumb trail as a bootstrap breadcrumb.
type BreadcrumbsBlock []Breadcrumb

func (b BreadcrumbsBlock) Render(rc RenderContext) (template.HTML, error) {
	return TemplateBlock{
		Name:     "block/breadcrumbs",
		Model:    b,
		Skip:     len(b) == 0,
		Template: shell,
	}.Render(rc)
}

// NavigationBlock renders the persistent navigation links as a bootstrap nav bar.
type NavigationBlock []NavLink

func (b NavigationBlock) Render(rc RenderContext) (template.HTML, error) {
	return TemplateBlock{
		Name:     "block/navigation",
		Model:    b,
		Skip:     len(b) == 0,
		Template: shell,
	}.Render(rc)
}
