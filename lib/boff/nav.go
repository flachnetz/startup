package boff

import (
	"encoding/base64"
	"encoding/json"
	"html/template"
	"net/http"
	"path"
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
// already known. BreadcrumbRemote should be prefered.
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

// breadcrumbCookie is the JSON-serialized content stored in the breadcrumb
// cookie.
type breadcrumbCookie struct {
	Breadcrumbs []Breadcrumb `json:"e"`
}

// navigationCookie is the JSON-serialized content stored in the navigation
// cookie.
type navigationCookie struct {
	Links []NavLink `json:"n"`
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
	known := make(map[string]Breadcrumb, len(c.Breadcrumbs))
	for _, entry := range c.Breadcrumbs {
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
// with the given breadcrumb chain (see BreadcrumbCookie.Reconcile), saves
// the result back to the cookie, and returns the reconciled breadcrumbs.
func ReconcileBreadcrumbs(c *echo.Context, entries ...Breadcrumb) []Breadcrumb {
	cookie, _ := loadBreadcrumbCookie(c)

	reconciled := cookie.Reconcile(entries)
	reconciled.Save(c)

	return reconciled.Breadcrumbs
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
	raw, err := json.Marshal(bc)
	if err != nil {
		// Breadcrumbs only ever contain plain strings, so this can't realistically fail.
		panic(err)
	}

	c.SetCookie(&http.Cookie{
		Name:     breadcrumbCookieName,
		Value:    base64.RawURLEncoding.EncodeToString(raw),
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

	updateNavigationIsActive(c, nc.Links)

	return nc, true
}

func updateNavigationIsActive(c *echo.Context, nav []NavLink) {
	currentPath := path.Clean(getRealPathFromContext(c))
	for i := range nav {
		if nav[i].Path == "" {
			continue
		}

		nav[i].IsActive = strings.HasPrefix(currentPath, path.Clean(nav[i].Path))
	}
}

func getRealPathFromContext(c *echo.Context) string {
	if path := c.Request().Header.Get("Real-Path"); path != "" {
		return path
	}

	return c.Request().URL.Path
}

// LoadNavigation returns the persistent top-level navigation links stored
// in the navigation cookie. It returns nil if the cookie is missing or
// could not be decoded.
func LoadNavigation(c *echo.Context) []NavLink {
	cookie, _ := loadNavigationCookie(c)
	return cookie.Links
}

// Save encodes the navigationCookie and writes it as the navigation cookie
// on the response.
func (nc navigationCookie) Save(c *echo.Context) {
	raw, err := json.Marshal(nc)
	if err != nil {
		// Links only ever contain plain strings, so this can't realistically fail.
		panic(err)
	}

	c.SetCookie(&http.Cookie{
		Name:     navigationCookieName,
		Value:    base64.RawURLEncoding.EncodeToString(raw),
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
}

// UpdateNavigation writes links to the navigation cookie as the persistent
// top-level navigation, replacing whatever was stored before.
func UpdateNavigation(c *echo.Context, links []NavLink) {
	navigationCookie{Links: links}.Save(c)

	updateNavigationIsActive(c, links)
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
