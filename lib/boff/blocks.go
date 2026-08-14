// Package boff holds the block-based rendering machinery used by backoffice
// pages. A page is a slice of Block; each block renders to a fragment of HTML,
// and the blocks are concatenated top to bottom into a page shell. This is the
// generic core, free of any particular page's models, so it can back a history
// ledger, an overview list, or any other backoffice page.
package boff

import (
	"bytes"
	"fmt"
	"html/template"
)

// Block is one renderable section of a page. Blocks are rendered top to bottom
// in the order given, so a page is just a slice of them. Providing your own
// Block is how you extend a page beyond its built-in sections without touching
// the page shell.
//
// A block is free to produce any HTML it likes. An empty block returns no bytes
// and is simply skipped.
type Block interface {
	Render() (template.HTML, error)
}

// TemplateBlock renders a named (sub-)template with a model. It is the common
// shape of the built-in blocks and the easiest way to add your own: give it the
// Name of a template and a Model to execute it with.
//
// Name is looked up in Template. When Template is nil, RenderBlocks fills it in
// with the shell template it is given, so Name can refer to a sub-template
// defined alongside the shell; set Template yourself to render against your own.
// Set Empty to render nothing, which is how a section vanishes when it has no
// content.
type TemplateBlock struct {
	Name     string
	Model    any
	Empty    bool
	Template *template.Template
}

func (b TemplateBlock) Render() (template.HTML, error) {
	if b.Empty {
		return "", nil
	}

	if b.Template == nil {
		return "", fmt.Errorf("render block %q: no template", b.Name)
	}

	var buf bytes.Buffer
	if err := b.Template.ExecuteTemplate(&buf, b.Name, b.Model); err != nil {
		return "", fmt.Errorf("render block %q: %w", b.Name, err)
	}

	return template.HTML(buf.String()), nil //nolint:gosec // template output is already escaped
}

// HTMLBlock is a block of pre-rendered HTML, for callers that want to drop in
// arbitrary markup without a template.
type HTMLBlock template.HTML

func (b HTMLBlock) Render() (template.HTML, error) {
	return template.HTML(b), nil
}

// RenderedBlock is a Block after Render, carried into a page model so the shell
// only concatenates the HTML.
type RenderedBlock struct{ HTML template.HTML }

// RenderBlocks renders every block, dropping the ones that produce no output.
// tpl is the shell template supplied to any TemplateBlock that does not carry
// its own, so its blocks can reference sub-templates defined alongside the
// shell.
func RenderBlocks(tpl *template.Template, blocks []Block) ([]RenderedBlock, error) {
	rendered := make([]RenderedBlock, 0, len(blocks))
	for _, block := range blocks {
		if tb, ok := block.(TemplateBlock); ok && tb.Template == nil {
			tb.Template = tpl
			block = tb
		}

		html, err := block.Render()
		if err != nil {
			return nil, err
		}

		if html == "" {
			continue
		}

		rendered = append(rendered, RenderedBlock{HTML: html})
	}

	return rendered, nil
}
