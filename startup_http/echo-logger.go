package startup_http

import (
	"context"
	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/labstack/gommon/log"
	"github.com/sirupsen/logrus"
	"io"
)

// Logger Wraps logrus logger for http echo framework
type Logger struct {
	*logrus.Entry
}

func (l *Logger) SetHeader(h string) {
}

func (l *Logger) Level() log.Lvl {
	switch l.Logger.Level {
	case logrus.DebugLevel:
		return log.DEBUG
	case logrus.WarnLevel:
		return log.WARN
	case logrus.ErrorLevel:
		return log.ERROR
	case logrus.InfoLevel:
		return log.INFO
	default:
		l.Panic("Invalid level")
	}

	return log.OFF
}

func (l *Logger) SetPrefix(s string) {
	// TODO
}

func (l *Logger) Prefix() string {
	// TODO.  Is this even valid?  I'm not sure it can be translated since
	// logrus uses a Formatter interface.  Which seems to me to probably be
	// a better way to do it.
	return ""
}

func (l *Logger) SetLevel(lvl log.Lvl) {
	switch lvl {
	case log.DEBUG:
		logrus.SetLevel(logrus.DebugLevel)
	case log.WARN:
		logrus.SetLevel(logrus.WarnLevel)
	case log.ERROR:
		logrus.SetLevel(logrus.ErrorLevel)
	case log.INFO:
		logrus.SetLevel(logrus.InfoLevel)
	default:
		l.Panic("Invalid level")
	}
}

func (l *Logger) Output() io.Writer {
	return l.Logger.Out
}

func (l *Logger) SetOutput(w io.Writer) {
	logrus.SetOutput(w)
}

func (l *Logger) Printj(j log.JSON) {
	logrus.WithFields(logrus.Fields(j)).Print()
}

func (l *Logger) Debugj(j log.JSON) {
	logrus.WithFields(logrus.Fields(j)).Debug()
}

func (l *Logger) Infoj(j log.JSON) {
	logrus.WithFields(logrus.Fields(j)).Info()
}

func (l *Logger) Warnj(j log.JSON) {
	logrus.WithFields(logrus.Fields(j)).Warn()
}

func (l *Logger) Errorj(j log.JSON) {
	logrus.WithFields(logrus.Fields(j)).Error()
}

func (l *Logger) Fatalj(j log.JSON) {
	logrus.WithFields(logrus.Fields(j)).Fatal()
}

func (l *Logger) Panicj(j log.JSON) {
	logrus.WithFields(logrus.Fields(j)).Panic()
}

func LoggerContextWithFields(ctx context.Context, object interface{}, playerId string, roundId string) context.Context {

	return startup_logrus.WithLogger(ctx, startup_logrus.GetLogger(ctx, object).WithField("playerId", playerId).WithField("roundId", roundId))
}
