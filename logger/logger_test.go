package logger_test

import (
	"bytes"
	"log"
	"sync"
	"testing"

	assertions "github.com/stretchr/testify/assert"

	"github.com/shaelmaar/tasks/logger"
)

func TestSimpleLogger(t *testing.T) {
	var b bytes.Buffer
	stdLogger := log.New(&b, "", log.LstdFlags)
	logger.SetDefault(logger.NewSimpleLogger(stdLogger, logger.LevelInfo))

	assert := assertions.New(t)

	logger.Debug("Debug")
	assert.Empty(&b)
	logger.Debugf("Debug%s", "f")
	assert.Empty(&b)

	logger.Info("Info")
	assert.NotEmpty(&b)
	logger.Infof("Info%s", "f")
	assert.NotEmpty(&b)

	logger.Warn("Warn")
	assert.NotEmpty(&b)
	logger.Warnf("Warn%s", "f")
	assert.NotEmpty(&b)

	logger.Error("Error")
	assert.NotEmpty(&b)
	logger.Errorf("Error%s", "f")
	assert.NotEmpty(&b)
}

func TestLoggerRace(t *testing.T) {
	var b bytes.Buffer
	stdLogger := log.New(&b, "", log.LstdFlags)

	logger1 := logger.NewSimpleLogger(stdLogger, logger.LevelDebug)
	logger2 := logger.NewSimpleLogger(stdLogger, logger.LevelInfo)
	logger3 := logger.NewSimpleLogger(stdLogger, logger.LevelWarn)

	wg := sync.WaitGroup{}
	wg.Add(3)
	go setLogger(&wg, logger1)
	go setLogger(&wg, logger2)
	go setLogger(&wg, logger3)
	wg.Wait()
	wg.Add(1)
	go setLogger(&wg, logger2)
	wg.Wait()

	if logger.Default() != logger2 {
		t.Fatal("logger set race error")
	}
}

func TestCustomLogger(t *testing.T) {
	l := &countingLogger{}
	logger.SetDefault(l)
	logger.Debug("debug")
	logger.Info("info")
	logger.Error("error")

	assert := assertions.New(t)
	assert.EqualValues(3, l.Count)
}

func TestLogFormat(t *testing.T) {
	var b bytes.Buffer
	stdLogger := log.New(&b, "", log.LstdFlags)
	simpleLogger := logger.NewSimpleLogger(stdLogger, logger.LevelDebug)
	logger.SetDefault(simpleLogger)

	assert := assertions.New(t)

	empty := struct{}{}
	simpleLogger.Debug("Debug")
	assert.NotEmpty(&b)
	simpleLogger.Debugf("Debugf: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")
	logger.Debugf("Infof: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")

	simpleLogger.Infof("Infof: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")
	logger.Infof("Infof: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")

	simpleLogger.Warnf("Warnf: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")
	logger.Warnf("Warnf: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")

	simpleLogger.Errorf("Errorf: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")
	logger.Errorf("Errorf: %s, %d, %v, %v", "a", 1, true, empty)
	assert.Contains(b.String(), "a, 1, true, {}")
}

func setLogger(wg *sync.WaitGroup, l *logger.SimpleLogger) {
	defer wg.Done()
	logger.SetDefault(l)
}

type countingLogger struct {
	Count int
}

var _ logger.Logger = (*countingLogger)(nil)

func (l *countingLogger) Trace(_ any) {
	l.Count++
}

func (l *countingLogger) Tracef(_ string, _ ...any) {
	l.Count++
}

func (l *countingLogger) Debug(_ any) {
	l.Count++
}

func (l *countingLogger) Debugf(_ string, _ ...any) {
	l.Count++
}

func (l *countingLogger) Info(_ any) {
	l.Count++
}

func (l *countingLogger) Infof(_ string, _ ...any) {
	l.Count++
}

func (l *countingLogger) Warn(_ any) {
	l.Count++
}

func (l *countingLogger) Warnf(_ string, _ ...any) {
	l.Count++
}

func (l *countingLogger) Error(_ any) {
	l.Count++
}

func (l *countingLogger) Errorf(_ string, _ ...any) {
	l.Count++
}

func (l *countingLogger) Enabled(_ logger.Level) bool {
	return true
}
