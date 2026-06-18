package logger

import (
	"io"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Config struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Init(level string, format string) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	logLevel := parseLevel(level)
	zerolog.SetGlobalLevel(logLevel)

	var writer io.Writer
	if strings.ToLower(format) == "console" {
		writer = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}
	} else {
		writer = os.Stdout
	}

	log.Logger = zerolog.New(writer).With().
		Timestamp().
		Str("service", "cache-proxy").
		Caller().
		Logger()
}

func parseLevel(level string) zerolog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	case "panic":
		return zerolog.PanicLevel
	case "trace":
		return zerolog.TraceLevel
	default:
		return zerolog.InfoLevel
	}
}

func Debug() *zerolog.Event {
	return log.Debug()
}

func Info() *zerolog.Event {
	return log.Info()
}

func Warn() *zerolog.Event {
	return log.Warn()
}

func Error() *zerolog.Event {
	return log.Error()
}

func Fatal() *zerolog.Event {
	return log.Fatal()
}

func With() zerolog.Context {
	return log.With()
}
