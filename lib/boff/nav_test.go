package boff

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBreadcrumbCookie_Reconcile_FillsInMissingLabelAndPathFromCache(t *testing.T) {
	cookie := breadcrumbCookie{Breadcrumbs: []Breadcrumb{
		BreadcrumbLocal("player:abc", "Player abc", "/players/abc"),
	}}

	result := cookie.Reconcile([]Breadcrumb{
		BreadcrumbRemote("player:abc"),
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	})

	require.Equal(t, []Breadcrumb{
		BreadcrumbLocal("player:abc", "Player abc", "/players/abc"),
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	}, result.Breadcrumbs)
}

func TestBreadcrumbCookie_Reconcile_KeepsAncestorEntryWithoutLinkIfNotCached(t *testing.T) {
	cookie := breadcrumbCookie{}

	result := cookie.Reconcile([]Breadcrumb{
		BreadcrumbRemote("player:abc"),
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	})

	require.Equal(t, []Breadcrumb{
		BreadcrumbRemote("player:abc"),
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	}, result.Breadcrumbs)
}

func TestBreadcrumbCookie_Reconcile_UpdatesCachedLabelAndPathWhenNewValuesAreProvided(t *testing.T) {
	cookie := breadcrumbCookie{Breadcrumbs: []Breadcrumb{
		BreadcrumbLocal("player:abc", "Old Label", "/old/path"),
	}}

	result := cookie.Reconcile([]Breadcrumb{
		BreadcrumbLocal("player:abc", "New Label", "/new/path"),
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	})

	require.Equal(t, []Breadcrumb{
		BreadcrumbLocal("player:abc", "New Label", "/new/path"),
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	}, result.Breadcrumbs)
}

func TestBreadcrumbCookie_Reconcile_LastEntryIsAlwaysAuthoritativeEvenIfCached(t *testing.T) {
	cookie := breadcrumbCookie{Breadcrumbs: []Breadcrumb{
		BreadcrumbLocal("order:123", "Old Order Label", "/old/orders/123"),
	}}

	result := cookie.Reconcile([]Breadcrumb{
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	})

	require.Equal(t, []Breadcrumb{
		BreadcrumbLocal("order:123", "Order 123", "/orders/123"),
	}, result.Breadcrumbs)
}
