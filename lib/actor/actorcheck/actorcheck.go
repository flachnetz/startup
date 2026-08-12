// Package actorcheck is the structural guard rail for the actor design: the
// actor is audit information, and a service must never make an authorization
// decision from it.
//
// Directories named testdata, vendor and node_modules are skipped, and so are
// _test.go files: a test may legitimately assert header handling.
//
// A service repository wires it into one test:
//
//	func TestBDR002R1_ActorIsNeverAnAuthorizationInput(t *testing.T) {
//		violations, err := actorcheck.Check(".")
//		require.NoError(t, err)
//		require.Empty(t, violations, "%v", violations)
//	}
//
// DEV-NOTE: see BauerMediaGroup-Stardust/platform-gitops
// docs/plans/keycloak-service-auth.md section 8 and BDR-002. The moment a
// handler branches on Actor-Id, any service holding a write role can
// impersonate any human, and four-eyes approval becomes a formality. Four-eyes
// in particular must read the token subject (jwt.Identity.Subject).
package actorcheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
)

// Violation is one place where an actor header value reaches a decision.
type Violation struct {
	File string
	Line int
	Expr string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s:%d: actor header value used in a decision: %s", v.File, v.Line, v.Expr)
}

// Check parses every non-test Go file under root and reports the places where a
// value read from an Actor-* header (HTTP or Kafka) is used in a condition.
//
// Reading the headers is fine, and so is comparing a header KEY to "Actor-Id"
// or testing a value for emptiness: recording provenance requires both. What is
// forbidden is branching on the value itself.
//
// ponytail: taint tracking is one level deep inside a single function, which
// covers the realistic mistake (read, then branch). Laundering the value through
// another function is not detected; the BDR carries that limitation in writing.
func Check(root string) ([]Violation, error) {
	var violations []Violation

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return fs.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		fileViolations, err := checkFile(path)
		if err != nil {
			return err
		}

		violations = append(violations, fileViolations...)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}

	return violations, nil
}

func checkFile(path string) ([]Violation, error) {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	var violations []Violation

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		tainted := taintedIdents(fn.Body)
		if len(tainted) == 0 {
			continue
		}

		for _, cond := range conditions(fn.Body) {
			ident := decidesOn(cond, tainted)
			if ident == "" {
				continue
			}

			position := fset.Position(cond.Pos())
			violations = append(violations, Violation{
				File: path,
				Line: position.Line,
				Expr: ident,
			})
		}
	}

	return violations, nil
}

// taintedIdents collects the names assigned from an actor header read.
func taintedIdents(body *ast.BlockStmt) map[string]bool {
	tainted := map[string]bool{}

	record := func(lhs []ast.Expr, rhs []ast.Expr) {
		if !containsActorRead(rhs) {
			return
		}

		for _, target := range lhs {
			if ident, ok := target.(*ast.Ident); ok && ident.Name != "_" {
				tainted[ident.Name] = true
			}
		}
	}

	ast.Inspect(body, func(node ast.Node) bool {
		switch stmt := node.(type) {
		case *ast.AssignStmt:
			record(stmt.Lhs, stmt.Rhs)
		case *ast.ValueSpec:
			names := make([]ast.Expr, 0, len(stmt.Names))
			for _, name := range stmt.Names {
				names = append(names, name)
			}
			record(names, stmt.Values)
		}

		return true
	})

	return tainted
}

// containsActorRead reports whether any expression reads an actor header, i.e.
// mentions an Actor-* header name or one of the header constants.
func containsActorRead(exprs []ast.Expr) bool {
	found := false

	for _, expr := range exprs {
		ast.Inspect(expr, func(node ast.Node) bool {
			if isActorHeaderName(node) {
				found = true
			}

			return !found
		})
	}

	return found
}

// isActorHeaderName matches both the literal header names and the exported
// HeaderActor* constants, whatever package they are referenced through.
func isActorHeaderName(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.BasicLit:
		if n.Kind != token.STRING {
			return false
		}

		value, err := strconv.Unquote(n.Value)
		if err != nil {
			return false
		}

		return strings.HasPrefix(strings.ToLower(value), "actor-")

	case *ast.Ident:
		return strings.HasPrefix(n.Name, "HeaderActor")

	case *ast.SelectorExpr:
		return strings.HasPrefix(n.Sel.Name, "HeaderActor")
	}

	return false
}

// conditions returns every expression a control flow decision is made on.
func conditions(body *ast.BlockStmt) []ast.Expr {
	var found []ast.Expr

	ast.Inspect(body, func(node ast.Node) bool {
		switch stmt := node.(type) {
		case *ast.IfStmt:
			if stmt.Cond != nil {
				found = append(found, stmt.Cond)
			}
		case *ast.SwitchStmt:
			if stmt.Tag != nil {
				found = append(found, stmt.Tag)
			}
		}

		return true
	})

	return found
}

// decidesOn returns the tainted identifier the condition branches on, or "".
// A comparison against the empty string is a presence check, not a decision
// about the principal, and a comparison where an Actor-* header name is an
// operand is key matching while reading headers.
func decidesOn(cond ast.Expr, tainted map[string]bool) string {
	if binary, ok := cond.(*ast.BinaryExpr); ok {
		switch binary.Op {
		case token.LAND, token.LOR:
			if ident := decidesOn(binary.X, tainted); ident != "" {
				return ident
			}

			return decidesOn(binary.Y, tainted)

		case token.EQL, token.NEQ:
			if isEmptyString(binary.X) || isEmptyString(binary.Y) {
				return ""
			}

			if isActorHeaderName(binary.X) || isActorHeaderName(binary.Y) {
				return ""
			}
		}
	}

	found := ""

	ast.Inspect(cond, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if ok && tainted[ident.Name] {
			found = ident.Name
			return false
		}

		return true
	})

	return found
}

func isEmptyString(expr ast.Expr) bool {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}

	value, err := strconv.Unquote(lit.Value)

	return err == nil && value == ""
}
