package boff

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

func scopedContext(scope, realPath string, cookies ...*http.Cookie) (*echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(http.MethodGet, "/internal/app/x", http.NoBody)
	if scope != "" {
		req.Header.Set(scopePathHeader, scope)
	}

	if realPath != "" {
		req.Header.Set("Real-Path", realPath)
	}

	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}

	rec := httptest.NewRecorder()

	return echo.New().NewContext(req, rec), rec
}

func TestScopedPath(t *testing.T) {
	c, _ := scopedContext("/op/acme/all/", "")

	require.Equal(t, "/op/acme/all/players/view/1", ScopedPath(c, "/players/view/1"))
	require.Equal(t, "/op/acme/all/orders/list?player_id=1", ScopedPath(c, "/orders/list?player_id=1"))
	require.Equal(t, "/op/acme/all/players", ScopedPath(c, "/op/acme/all/players"), "idempotent")
	require.Equal(t, "/op/other/x/players", ScopedPath(c, "/op/other/x/players"), "foreign scope is left alone")
	require.Equal(t, "?page=2", ScopedPath(c, "?page=2"))
	require.Equal(t, "//evil.example/x", ScopedPath(c, "//evil.example/x"))
	require.Empty(t, ScopedPath(c, ""))
}

func TestScopedPath_WithoutValidScope(t *testing.T) {
	for _, scope := range []string{"", "/op/..//all", "/op/../all", "/op/a", "/x/a/b", "/op/a/b/c", "//evil.example", "/op/a b/all"} {
		c, _ := scopedContext(scope, "")
		require.Equal(t, "/players/x", ScopedPath(c, "/players/x"), scope)
	}
}

func TestUnscopedPath(t *testing.T) {
	require.Equal(t, "/players/x", unscopedPath("/op/acme/all/players/x"))
	require.Equal(t, "/", unscopedPath("/op/acme/all"))
	require.Equal(t, "/?a=1", unscopedPath("/op/acme/all?a=1"))
	require.Equal(t, "/players/x", unscopedPath("/players/x"))
	require.Empty(t, unscopedPath(""))
}

func setCookies(rec *httptest.ResponseRecorder) []*http.Cookie {
	return rec.Result().Cookies()
}

func TestNavigation_CookieHoldsNoScopeAndLinksGetTheScopeOfTheRequest(t *testing.T) {
	// A service passes a scoped path (built from Fragment-Base-Path) ...
	c, rec := scopedContext("/op/a/all", "/op/a/all/players/app/1")
	links := []NavLink{
		{Label: "Players", Path: "/op/a/all/players/app"},
		{Label: "Orders", Path: "/orders/app"},
	}
	UpdateNavigation(c, links)

	require.Equal(t, "/op/a/all/players/app", links[0].Path)
	require.Equal(t, "/op/a/all/orders/app", links[1].Path)
	require.True(t, links[0].IsActive)
	require.False(t, links[1].IsActive)

	// ... the cookie holds none, so another tab in another scope gets its own.
	stored := setCookies(rec)
	require.Len(t, stored, 1)

	other, _ := scopedContext("/op/b/shop1", "/op/b/shop1/orders/app/v1/orders/1", stored[0])
	loaded := LoadNavigation(other)

	require.Equal(t, "/op/b/shop1/players/app", loaded[0].Path)
	require.Equal(t, "/op/b/shop1/orders/app", loaded[1].Path)
	require.False(t, loaded[0].IsActive)
	require.True(t, loaded[1].IsActive)
}

func TestNavigation_WithoutScope(t *testing.T) {
	c, rec := scopedContext("", "/players/app/1")
	links := []NavLink{{Label: "Players", Path: "/players/app"}}
	UpdateNavigation(c, links)

	require.Equal(t, "/players/app", links[0].Path)
	require.True(t, links[0].IsActive)

	other, _ := scopedContext("", "/orders/app", setCookies(rec)...)
	require.Equal(t, "/players/app", LoadNavigation(other)[0].Path)
}

func TestBreadcrumbs_CookieHoldsNoScope(t *testing.T) {
	c, rec := scopedContext("/op/a/all", "")
	trail := ReconcileBreadcrumbs(c,
		BreadcrumbLocal("player:1", "Player 1", "/op/a/all/players/app/1"),
		BreadcrumbLocal("order:1", "Order 1", "/orders/app/v1/orders/1"),
	)

	require.Equal(t, "/op/a/all/players/app/1", trail[0].Path)
	require.Equal(t, "/op/a/all/orders/app/v1/orders/1", trail[1].Path)

	// A page in another scope that only knows the ancestor by key.
	other, _ := scopedContext("/op/b/all", "", setCookies(rec)...)
	trail = ReconcileBreadcrumbs(other,
		BreadcrumbRemote("player:1"),
		BreadcrumbLocal("payment:1", "Payment 1", "/payments/app/v1/payments/1"),
	)

	require.Equal(t, "/op/b/all/players/app/1", trail[0].Path)
	require.Equal(t, "/op/b/all/payments/app/v1/payments/1", trail[1].Path)
}
