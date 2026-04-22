package startup_failpoints

import (
	"embed"
	"html/template"
	"io"
	"strings"

	"fmt"
)

//go:embed templates/failpoint.gohtml
var fpTemplate embed.FS

type TemplateResponse struct {
	UpdateFailPointsEndpoint string
	FailPoints               []FailPoint
	FailPointLocations       map[FailPointLocation]FailPoint
}

func renderIndex(w io.Writer, model any) error {
	t := template.New("failpoint.gohtml").Funcs(
		template.FuncMap{
			"join": strings.Join,
		},
	)
	tmpl, err := t.ParseFS(fpTemplate, "templates/failpoint.gohtml")
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	if err = tmpl.Execute(w, model); err != nil {
		return fmt.Errorf("executing template: %w", err)
	}

	return nil
}
