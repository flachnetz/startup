package startup_failpoints

import (
	"embed"
	"html/template"
	"io"
	"strings"

	"github.com/pkg/errors"
)

//go:embed templates/failpoint.gohtml
var fpTemplate embed.FS

type TemplateResponse struct {
	UpdateFailPointsEndpoint string
	FailPoints               []FailPoint
	FailPointLocations       map[FailPointLocation]FailPoint
}

func renderIndex(w io.Writer, model interface{}) error {
	t := template.New("failpoint.gohtml").Funcs(
		template.FuncMap{
			"join": strings.Join,
		},
	)
	tmpl, err := t.ParseFS(fpTemplate, "templates/failpoint.gohtml")
	if err != nil {
		return errors.WithMessage(err, "parsing template")
	}

	if err = tmpl.Execute(w, model); err != nil {
		return errors.WithMessage(err, "executing template")
	}

	return nil
}
