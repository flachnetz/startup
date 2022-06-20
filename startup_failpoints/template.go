package failpoints

import (
	"embed"
	"github.com/pkg/errors"
	"html/template"
	"io"
)

var (
	//go:embed templates/failpoint.gohtml
	fpTemplate embed.FS
)

type TemplateResponse struct {
	UpdateFailPointsEndpoint string
	FailPoints               []FailPoint
	FailPointLocations       map[FailPointLocation]FailPoint
}

func renderIndex(w io.Writer, model interface{}) error {
	t := template.New("failpoint.gohtml")
	tmpl, err := t.ParseFS(fpTemplate, "templates/failpoint.gohtml")
	if err != nil {
		return errors.WithMessage(err, "parsing template")
	}

	if err = tmpl.Execute(w, model); err != nil {
		return errors.WithMessage(err, "executing template")
	}

	return nil
}
