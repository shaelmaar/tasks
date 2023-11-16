package logger

import (
	"log"
)

// SimpleLogger prefixes.
const (
	DebugPrefix = "DEBUG "
	InfoPrefix  = "INFO "
	WarnPrefix  = "WARN "
	ErrorPrefix = "ERROR "
)

// SimpleLogger implements the logger.Logger interface.
type SimpleLogger struct {
	logger *log.Logger
	level  Level
}

var _ Logger = (*SimpleLogger)(nil)

// NewSimpleLogger returns a new SimpleLogger.
func NewSimpleLogger(logger *log.Logger, level Level) *SimpleLogger {
	return &SimpleLogger{
		logger: logger,
		level:  level,
	}
}

// Debug logs at LevelDebug.
// Arguments are handled in the manner of fmt.Println.
func (l *SimpleLogger) Debug(msg any) {
	if l.enabled(LevelDebug) {
		l.logger.SetPrefix(DebugPrefix)
		l.logger.Println(msg)
	}
}

// Debugf logs at LevelDebug.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Debugf(format string, args ...any) {
	if l.enabled(LevelDebug) {
		l.logger.SetPrefix(DebugPrefix)
		l.logger.Printf(format, args...)
	}
}

// Info logs at LevelInfo.
// Arguments are handled in the manner of fmt.Println.
func (l *SimpleLogger) Info(msg any) {
	if l.enabled(LevelInfo) {
		l.logger.SetPrefix(InfoPrefix)
		l.logger.Println(msg)
	}
}

// Infof logs at LevelInfo.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Infof(format string, args ...any) {
	if l.enabled(LevelInfo) {
		l.logger.SetPrefix(InfoPrefix)
		l.logger.Printf(format, args...)
	}
}

// Warn logs at LevelWarn.
// Arguments are handled in the manner of fmt.Println.
func (l *SimpleLogger) Warn(msg any) {
	if l.enabled(LevelWarn) {
		l.logger.SetPrefix(WarnPrefix)
		l.logger.Println(msg)
	}
}

// Warnf logs at LevelWarn.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Warnf(format string, args ...any) {
	if l.enabled(LevelWarn) {
		l.logger.SetPrefix(WarnPrefix)
		l.logger.Printf(format, args...)
	}
}

// Error logs at LevelError.
// Arguments are handled in the manner of fmt.Println.
func (l *SimpleLogger) Error(msg any) {
	if l.enabled(LevelError) {
		l.logger.SetPrefix(ErrorPrefix)
		l.logger.Println(msg)
	}
}

// Errorf logs at LevelError.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Errorf(format string, args ...any) {
	if l.enabled(LevelError) {
		l.logger.SetPrefix(ErrorPrefix)
		l.logger.Printf(format, args...)
	}
}

// enabled reports whether the logger handles records at the given level.
func (l *SimpleLogger) enabled(level Level) bool {
	return level >= l.level
}
