package history

import (
	"io"

	"github.com/flachnetz/startup/v2/lib/boff"
)

// These aliases preserve the old history.* names after the types moved to
// lib/boff. Each carries a //go:fix inline directive so `go fix` (and gopls)
// rewrites references to the boff.* target and the aliases can later be removed.

//go:fix inline
type Action = boff.Action

//go:fix inline
type SummaryItem = boff.SummaryItem

//go:fix inline
type OverviewFilter = boff.OverviewFilter

//go:fix inline
type FilterOption = boff.FilterOption

//go:fix inline
type OverviewConfig = boff.OverviewConfig

//go:fix inline
type OverviewRow = boff.OverviewRow

//go:fix inline
type OverviewCell = boff.OverviewCell

//go:fix inline
const PageParam = boff.PageParam

// RenderOverview forwards to boff.RenderOverview.
//
//go:fix inline
func RenderOverview(w io.Writer, title string, headers []string, rows []OverviewRow) error {
	return boff.RenderOverview(w, title, headers, rows)
}

// RenderOverviewWithConfig forwards to boff.RenderOverviewWithConfig.
//
//go:fix inline
func RenderOverviewWithConfig(w io.Writer, cfg OverviewConfig) error {
	return boff.RenderOverviewWithConfig(w, cfg)
}
