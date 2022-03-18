package dataloader

type Logger interface {
	Printf(format string, v ...any)
}

type NoopLogger struct{}

func (NoopLogger) Printf(format string, v ...any) {}
