package logger

type Logger interface {
	// Debug logs at LevelDebug.
	Debug(args ...any)

	// Debugf logs at LevelDebug.
	Debugf(format string, args ...any)

	// Info logs at LevelInfo.
	Info(args ...any)

	// Infof logs at LevelInfo.
	Infof(format string, args ...any)

	// Warn logs at LevelWarn.
	Warn(args ...any)

	// Warnf logs at LevelWarn.
	Warnf(format string, args ...any)

	// Error logs at LevelError.
	Error(args ...any)

	// Errorf logs at LevelError.
	Errorf(format string, args ...any)
}

// A Level is the importance or severity of a log event.
// The higher the level, the more important or severe the event.
type Level int

// Names for common log levels.
const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)
