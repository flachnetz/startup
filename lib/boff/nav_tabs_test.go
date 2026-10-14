package boff

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

// jar is the cookie store shared by all tabs of one browser.
type jar map[string]*http.Cookie

// page serves one request of a tab: it sends the jar's cookies, runs fn and
// keeps the cookies the response sets.
func (j jar) page(scope, realPath string, fn func(c *echo.Context)) {
	req := httptest.NewRequest(http.MethodGet, "/internal/x", http.NoBody)
	req.Header.Set(scopePathHeader, scope)
	req.Header.Set("Real-Path", realPath)

	for _, cookie := range j {
		req.AddCookie(cookie)
	}

	rec := httptest.NewRecorder()
	fn(echo.New().NewContext(req, rec))

	for _, cookie := range rec.Result().Cookies() {
		j[cookie.Name] = cookie
	}
}

func playerNav(id string) []NavLink {
	return []NavLink{
		{Label: "Orders", Path: "/orders/app?player_id=" + id},
		{Label: "Limits", Path: "/limits/app/player/" + id},
	}
}

func TestTabsOnDifferentSubjectsKeepTheirOwnNavigationAndBreadcrumbs(t *testing.T) {
	j := jar{}

	// Tab A opens player 1, tab B opens player 2.
	j.page("/op/a/all", "/op/a/all/players/app/player/1", func(c *echo.Context) {
		ReconcileBreadcrumbs(c,
			BreadcrumbLocal("players", "Players", "/players/app"),
			BreadcrumbLocal("player:1", "Player 1", "/players/app/player/1"))
		UpdateNavigation(c, playerNav("1"))
	})
	j.page("/op/a/all", "/op/a/all/players/app/player/2", func(c *echo.Context) {
		ReconcileBreadcrumbs(c,
			BreadcrumbLocal("players", "Players", "/players/app"),
			BreadcrumbLocal("player:2", "Player 2", "/players/app/player/2"))
		UpdateNavigation(c, playerNav("2"))
	})

	// Tab A follows a link into another service's page of player 1.
	j.page("/op/a/all", "/op/a/all/orders/app/v1/orders/o1/history", func(c *echo.Context) {
		trail := ReconcileBreadcrumbs(c,
			BreadcrumbRemote("players"),
			BreadcrumbRemote("player:1"),
			BreadcrumbLocal("order:o1", "Order o1", "/orders/app/v1/orders/o1/history"))

		require.Equal(t, "/op/a/all/players/app/player/1", trail[1].Path, "ancestor survives the other tab")
		require.Equal(t, "Player 1", trail[1].Label)

		nav := LoadNavigation(c)
		require.Len(t, nav, 2)
		require.Equal(t, "/op/a/all/orders/app?player_id=1", nav[0].Path)
	})

	// And tab B into one of player 2.
	j.page("/op/a/all", "/op/a/all/limits/app/player/2", func(c *echo.Context) {
		trail := ReconcileBreadcrumbs(c,
			BreadcrumbRemote("players"),
			BreadcrumbRemote("player:2"),
			BreadcrumbLocal("limits:2", "Limits", "/limits/app/player/2"))
		require.Equal(t, "/op/a/all/players/app/player/2", trail[1].Path)

		nav := LoadNavigation(c)
		require.Equal(t, "/op/a/all/orders/app?player_id=2", nav[0].Path)
		require.True(t, nav[1].IsActive)
	})
}

func TestNavigationOfAnotherSubjectIsNotShown(t *testing.T) {
	j := jar{}

	j.page("", "/players/app/player/1", func(c *echo.Context) {
		ReconcileBreadcrumbs(c, BreadcrumbLocal("player:1", "Player 1", "/players/app/player/1"))
		UpdateNavigation(c, playerNav("1"))
	})

	j.page("", "/draws/app/draw/9", func(c *echo.Context) {
		ReconcileBreadcrumbs(c, BreadcrumbLocal("draws", "Draws", "/draws/app"), BreadcrumbLocal("draw:9", "Draw 9", "/draws/app/draw/9"))
		require.Empty(t, LoadNavigation(c))
	})
}

func TestUpdateNavigationWithoutLinksRemovesTheSubjectsSet(t *testing.T) {
	j := jar{}
	visit := func(links []NavLink) {
		j.page("", "/players/app/player/1", func(c *echo.Context) {
			ReconcileBreadcrumbs(c, BreadcrumbLocal("player:1", "Player 1", "/players/app/player/1"))
			UpdateNavigation(c, links)
		})
	}

	visit(playerNav("1"))
	visit(nil)

	j.page("", "/orders/app", func(c *echo.Context) {
		ReconcileBreadcrumbs(c, BreadcrumbRemote("player:1"))
		require.Empty(t, LoadNavigation(c))
	})
}

func TestBreadcrumbCacheIsBoundedAndKeepsTheMostRecent(t *testing.T) {
	j := jar{}

	for i := range 50 {
		j.page("", "/players/app/player/x", func(c *echo.Context) {
			ReconcileBreadcrumbs(c, BreadcrumbLocal(
				fmt.Sprintf("player:01JABCDEFGHJKMNPQRSTVWXY%02d", i),
				fmt.Sprintf("01JABCDEFGHJKMNPQRSTVWXY%02d", i),
				fmt.Sprintf("/players/app/player/01JABCDEFGHJKMNPQRSTVWXY%02d", i)))
		})
	}

	value := j[breadcrumbCookieName].Value
	require.LessOrEqual(t, len(value), maxCookieValueLen)

	raw, err := base64.RawURLEncoding.DecodeString(value)
	require.NoError(t, err)
	require.LessOrEqual(t, strings.Count(string(raw), `"k"`), maxBreadcrumbCache)

	j.page("", "/orders/app", func(c *echo.Context) {
		trail := ReconcileBreadcrumbs(c, BreadcrumbRemote("player:01JABCDEFGHJKMNPQRSTVWXY49"), BreadcrumbRemote("player:01JABCDEFGHJKMNPQRSTVWXY00"))
		require.NotEmpty(t, trail[0].Path, "newest entry is kept")
		require.Empty(t, trail[1].Path, "oldest entry is evicted")
	})
}

func TestNavigationSetsAreBounded(t *testing.T) {
	j := jar{}

	for i := range 20 {
		id := fmt.Sprintf("01JABCDEFGHJKMNPQRSTVWXY%02d", i)
		j.page("", "/players/app/player/"+id, func(c *echo.Context) {
			ReconcileBreadcrumbs(c, BreadcrumbLocal("player:"+id, id, "/players/app/player/"+id))
			UpdateNavigation(c, playerNav(id))
		})
	}

	require.LessOrEqual(t, len(j[navigationCookieName].Value), maxCookieValueLen)

	j.page("", "/orders/app", func(c *echo.Context) {
		ReconcileBreadcrumbs(c, BreadcrumbRemote("player:01JABCDEFGHJKMNPQRSTVWXY19"))
		require.NotEmpty(t, LoadNavigation(c))
	})
	j.page("", "/orders/app", func(c *echo.Context) {
		ReconcileBreadcrumbs(c, BreadcrumbRemote("player:01JABCDEFGHJKMNPQRSTVWXY00"))
		require.Empty(t, LoadNavigation(c))
	})
}

func TestCookieWrittenBeforeTheCacheExistedStillResolvesAncestors(t *testing.T) {
	legacy := `{"e":[{"k":"player:1","l":"Player 1","p":"/players/app/player/1"}]}`
	j := jar{breadcrumbCookieName: {Name: breadcrumbCookieName, Value: base64.RawURLEncoding.EncodeToString([]byte(legacy))}}

	j.page("", "/orders/app", func(c *echo.Context) {
		trail := ReconcileBreadcrumbs(c, BreadcrumbRemote("player:1"), BreadcrumbLocal("order:1", "Order 1", "/orders/app/1"))
		require.Equal(t, "/players/app/player/1", trail[0].Path)
	})
}
