/*
Package tint implements a zero-dependency [slog.Handler] that writes tinted
(colorized) logs. The output format is inspired by the [zerolog.ConsoleWriter]
and [slog.TextHandler].

The output format can be customized using [Options], which is a drop-in
replacement for [slog.HandlerOptions].

# Customize Attributes

Options.ReplaceAttr can be used to alter or drop attributes. If set, it is
called on each non-group attribute before it is logged.
See [slog.HandlerOptions] for details.

	w := os.Stderr
	logger := slog.New(
		tint.NewHandler(w, &tint.Options{
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey && len(groups) == 0 {
					return slog.Attr{}
				}
				return a
			},
		}),
	)

# Automatically Enable Colors

Colors are enabled by default and can be disabled using the Options.NoColor
attribute. To automatically enable colors based on the terminal capabilities,
use e.g. the [go-isatty] package.

	w := os.Stderr
	logger := slog.New(
		tint.NewHandler(w, &tint.Options{
			NoColor: !isatty.IsTerminal(w.Fd()),
		}),
	)

# Windows Support

Color support on Windows can be added by using e.g. the [go-colorable] package.

	w := os.Stderr
	logger := slog.New(
		tint.NewHandler(colorable.NewColorable(w), nil),
	)

[zerolog.ConsoleWriter]: https://pkg.go.dev/github.com/rs/zerolog#ConsoleWriter
[go-isatty]: https://pkg.go.dev/github.com/mattn/go-isatty
[go-colorable]: https://pkg.go.dev/github.com/mattn/go-colorable
*/
package tint

import (
	"context"
	"encoding"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// ANSI modes
const (
	ansiReset          = "\033[0m"
	ansiFaint          = "\033[2m"
	ansiResetFaint     = "\033[22m"
	ansiBrightRed      = "\033[91m"
	ansiBrightGreen    = "\033[92m"
	ansiBrightYellow   = "\033[93m"
	ansiBrightRedFaint = "\033[91;2m"
	ansiBlue           = "\033[34m"
)

const errKey = "err"

var (
	defaultLevel      = slog.LevelInfo
	defaultTimeFormat = time.StampMilli
)

// Options for a slog.Handler that writes tinted logs. A zero Options consists
// entirely of default values.
//
// Options can be used as a drop-in replacement for [slog.HandlerOptions].
type Options struct {
	// Enable source code location (Default: false)
	AddSource bool

	// Minimum level to log (Default: slog.LevelInfo)
	Level slog.Leveler

	// ReplaceAttr is called to rewrite each non-group attribute before it is logged.
	// See https://pkg.go.dev/log/slog#HandlerOptions for details.
	ReplaceAttr func(groups []string, attr slog.Attr) slog.Attr

	// Time format (Default: time.StampMilli)
	TimeFormat string

	// Disable color (Default: false)
	NoColor bool
}

// NewHandler creates a [slog.Handler] that writes tinted logs to Writer w,
// using the default options. If opts is nil, the default options are used.
func NewHandler(w io.Writer, opts *Options) slog.Handler {
	h := &handler{
		w:          w,
		level:      defaultLevel,
		timeFormat: defaultTimeFormat,
	}
	if opts == nil {
		return h
	}

	h.addSource = opts.AddSource
	if opts.Level != nil {
		h.level = opts.Level
	}
	h.replaceAttr = opts.ReplaceAttr
	if opts.TimeFormat != "" {
		h.timeFormat = opts.TimeFormat
	}
	h.noColor = opts.NoColor
	return h
}

// handler implements a [slog.Handler].
type handler struct {
	attrsPrefix string
	groupPrefix string
	groups      []string

	mu sync.Mutex
	w  io.Writer

	addSource   bool
	level       slog.Leveler
	replaceAttr func([]string, slog.Attr) slog.Attr
	timeFormat  string
	noColor     bool
}

func (h *handler) clone() *handler {
	return &handler{
		attrsPrefix: h.attrsPrefix,
		groupPrefix: h.groupPrefix,
		groups:      h.groups,
		w:           h.w,
		addSource:   h.addSource,
		level:       h.level,
		replaceAttr: h.replaceAttr,
		timeFormat:  h.timeFormat,
		noColor:     h.noColor,
	}
}

func (h *handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	// get a buffer from the sync pool
	buf := newBuffer()
	defer buf.Free()

	rep := h.replaceAttr

	// write time
	if !r.Time.IsZero() {
		val := r.Time.Round(0) // strip monotonic to match Attr behavior
		if rep == nil {
			h.appendTime(buf, r.Time)
			_ = buf.WriteByte(' ')
		} else if a := rep(h.groups, slog.Time(slog.TimeKey, val)); a.Key != "" {
			if a.Value.Kind() == slog.KindTime {
				h.appendTime(buf, r.Time)
			} else {
				appendValue(buf, a.Value, false)
			}
			_ = buf.WriteByte(' ')
		}
	}

	// write level
	if rep == nil {
		h.appendLevel(buf, r.Level)
		_ = buf.WriteByte(' ')
	} else if a := rep(h.groups, slog.Int(slog.LevelKey, int(r.Level))); a.Key != "" {
		if a.Value.Kind() == slog.KindInt64 {
			h.appendLevel(buf, slog.Level(a.Value.Int64()))
		} else {
			appendValue(buf, a.Value, false)
		}
		_ = buf.WriteByte(' ')
	}

	// write source
	if h.addSource {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		if f.File != "" {
			src := &slog.Source{
				Function: f.Function,
				File:     f.File,
				Line:     f.Line,
			}

			if rep == nil {
				h.appendSource(buf, src)
				_ = buf.WriteByte(' ')
			} else if a := rep(h.groups, slog.Any(slog.SourceKey, src)); a.Key != "" {
				appendValue(buf, a.Value, false)
				_ = buf.WriteByte(' ')
			}
		}
	} else {
		r.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "prefix" {
				h.appendSourceValue(buf, attr.Value)

				_ = buf.WriteByte(' ')

				// stop iteration
				return false
			}

			return true
		})
	}

	// write message
	if rep == nil {
		_, _ = buf.WriteString(r.Message)
		_ = buf.WriteByte(' ')
	} else if a := rep(h.groups, slog.String(slog.MessageKey, r.Message)); a.Key != "" {
		appendValue(buf, a.Value, false)
		_ = buf.WriteByte(' ')
	}

	// write handler attributes
	if len(h.attrsPrefix) > 0 {
		_, _ = buf.WriteString(h.attrsPrefix)
	}

	// write attributes
	r.Attrs(func(attr slog.Attr) bool {
		if rep != nil {
			attr = rep(h.groups, attr)
		}

		if attr.Key == "prefix" {
			return true
		}

		h.appendAttr(buf, attr, h.groupPrefix)
		return true
	})

	if len(*buf) == 0 {
		return nil
	}
	(*buf)[len(*buf)-1] = '\n' // replace last space with newline

	h.mu.Lock()
	defer h.mu.Unlock()

	_, err := h.w.Write(*buf)
	return err
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := h.clone()

	buf := newBuffer()
	defer buf.Free()

	// write attributes to buffer
	for _, attr := range attrs {
		if h.replaceAttr != nil {
			attr = h.replaceAttr(h.groups, attr)
		}

		if attr.Key == "prefix" {
			continue
		}

		h.appendAttr(buf, attr, h.groupPrefix)
	}
	h2.attrsPrefix = h.attrsPrefix + string(*buf)
	return h2
}

func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := h.clone()
	h2.groupPrefix += name + "."
	h2.groups = append(h2.groups, name)
	return h2
}

func (h *handler) appendTime(buf *buffer, t time.Time) {
	_, _ = buf.WriteStringIf(!h.noColor, ansiFaint)
	*buf = t.AppendFormat(*buf, h.timeFormat)
	_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
}

func (h *handler) appendLevel(buf *buffer, level slog.Level) {
	delta := func(buf *buffer, val slog.Level) {
		if val == 0 {
			return
		}
		_ = buf.WriteByte('+')
		*buf = strconv.AppendInt(*buf, int64(val), 10)
	}

	switch {
	case level < slog.LevelInfo:
		_, _ = buf.WriteStringIf(!h.noColor, ansiBlue)
		_, _ = buf.WriteString("DEBUG")
		delta(buf, level-slog.LevelDebug)
		_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
	case level < slog.LevelWarn:
		_, _ = buf.WriteStringIf(!h.noColor, ansiBrightGreen)
		_, _ = buf.WriteString(" INFO")
		delta(buf, level-slog.LevelInfo)
		_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
	case level < slog.LevelError:
		_, _ = buf.WriteStringIf(!h.noColor, ansiBrightYellow)
		_, _ = buf.WriteString(" WARN")
		delta(buf, level-slog.LevelWarn)
		_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
	default:
		_, _ = buf.WriteStringIf(!h.noColor, ansiBrightRed)
		_, _ = buf.WriteString("ERROR")
		delta(buf, level-slog.LevelError)
		_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
	}
}

func (h *handler) appendSource(buf *buffer, src *slog.Source) {
	_, file := filepath.Split(src.File)

	_, _ = buf.WriteStringIf(!h.noColor, ansiFaint)
	// buf.WriteString(filepath.Join(filepath.Base(dir), file))
	_, _ = buf.WriteString(file)
	_ = buf.WriteByte(':')
	_, _ = buf.WriteString(strconv.Itoa(src.Line))
	_ = buf.WriteByte(' ')

	fn := src.Function
	if idx := strings.LastIndexByte(fn, '/'); idx >= 0 {
		fn = fn[idx+1:]
	}
	_, _ = buf.WriteString(fn)

	_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
}

func (h *handler) appendSourceValue(buf *buffer, srcValue slog.Value) {
	_, _ = buf.WriteStringIf(!h.noColor, ansiFaint)
	appendValue(buf, srcValue, false)
	_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
}

func (h *handler) appendAttr(buf *buffer, attr slog.Attr, groupsPrefix string) {
	if attr.Equal(slog.Attr{}) {
		return
	}
	attr.Value = attr.Value.Resolve()

	switch attr.Value.Kind() {
	case slog.KindGroup:
		groupsPrefix += attr.Key + "."
		for _, groupAttr := range attr.Value.Group() {
			h.appendAttr(buf, groupAttr, groupsPrefix)
		}
	case slog.KindAny:
		if err, ok := attr.Value.Any().(tintError); ok {
			// append tintError
			h.appendTintError(buf, err, groupsPrefix)
			_ = buf.WriteByte(' ')
			break
		}
		fallthrough
	default:
		h.appendKey(buf, attr.Key, groupsPrefix)
		appendValue(buf, attr.Value, true)
		_ = buf.WriteByte(' ')
	}
}

func (h *handler) appendKey(buf *buffer, key, groups string) {
	_, _ = buf.WriteStringIf(!h.noColor, ansiFaint)
	appendString(buf, groups+key, true)
	_ = buf.WriteByte('=')
	_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
}

func appendValue(buf *buffer, v slog.Value, quote bool) {
	switch v.Kind() {
	case slog.KindString:
		appendString(buf, v.String(), quote)
	case slog.KindInt64:
		*buf = strconv.AppendInt(*buf, v.Int64(), 10)
	case slog.KindUint64:
		*buf = strconv.AppendUint(*buf, v.Uint64(), 10)
	case slog.KindFloat64:
		*buf = strconv.AppendFloat(*buf, v.Float64(), 'g', -1, 64)
	case slog.KindBool:
		*buf = strconv.AppendBool(*buf, v.Bool())
	case slog.KindDuration:
		appendString(buf, v.Duration().String(), quote)
	case slog.KindTime:
		appendString(buf, v.Time().String(), quote)
	case slog.KindAny:
		if tm, ok := v.Any().(encoding.TextMarshaler); ok {
			data, err := tm.MarshalText()
			if err != nil {
				break
			}
			appendString(buf, string(data), quote)
			break
		}
		appendString(buf, fmt.Sprint(v.Any()), quote)
	case slog.KindGroup:
	case slog.KindLogValuer:
	}
}

func (h *handler) appendTintError(buf *buffer, err error, groups string) {
	_, _ = buf.WriteStringIf(!h.noColor, ansiBrightRedFaint)
	appendString(buf, h.groupPrefix+groups+errKey, true)
	_ = buf.WriteByte('=')
	_, _ = buf.WriteStringIf(!h.noColor, ansiResetFaint)
	appendString(buf, err.Error(), true)
	_, _ = buf.WriteStringIf(!h.noColor, ansiReset)
}

func appendString(buf *buffer, s string, quote bool) {
	if quote && needsQuoting(s) {
		*buf = strconv.AppendQuote(*buf, s)
	} else {
		_, _ = buf.WriteString(s)
	}
}

func needsQuoting(s string) bool {
	if len(s) == 0 {
		return true
	}
	for _, r := range s {
		if unicode.IsSpace(r) || r == '"' || r == '=' || !unicode.IsPrint(r) {
			return true
		}
	}
	return false
}

type tintError struct{ error }

// Err returns a tinted (colorized) [slog.Attr] that will be written in red color
// by the [tint.Handler]. When used with any other [slog.Handler], it behaves as
//
//	slog.Any("err", err)
func Err(err error) slog.Attr {
	if err != nil {
		err = tintError{err}
	}
	return slog.Any(errKey, err)
}
