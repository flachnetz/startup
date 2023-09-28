package kafka

import (
	"fmt"
	"strings"

	logrus "github.com/sirupsen/logrus"
)

type logrusAdapter struct {
	delegate *logrus.Entry
}

func (l logrusAdapter) Print(v ...interface{}) {
	l.delegate.Println(strings.TrimSpace(fmt.Sprint(v...)))
}

func (l logrusAdapter) Printf(format string, v ...interface{}) {
	l.delegate.Println(strings.TrimSpace(fmt.Sprintf(format, v...)))
}

func (l logrusAdapter) Println(v ...interface{}) {
	l.delegate.Println(strings.TrimSpace(fmt.Sprint(v...)))
}
