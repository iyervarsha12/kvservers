package kvpaxos

import (
	"os"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// SetupLogger configures the global logger with the specified level and pretty printing.
func SetupLogger(logLevel int, prettyPrint bool) {
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))
	if prettyPrint {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}

func (kv *KVServer) Trace(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&kv.enableLogging)
	if enableLogging == 1 {
		log.Trace().Msgf("[KV SERVER=%d] "+format, append([]interface{}{kv.me}, a...)...)
	}
	return
}

func (kv *KVServer) Debug(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&kv.enableLogging)
	if enableLogging == 1 {
		log.Debug().Msgf("[KV SERVER=%d] "+format, append([]interface{}{kv.me}, a...)...)
	}
	return
}

func (kv *KVServer) Info(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&kv.enableLogging)
	if enableLogging == 1 {
		log.Info().Msgf("[KV SERVER=%d] "+format, append([]interface{}{kv.me}, a...)...)
	}
	return
}

func (kv *KVServer) Warn(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&kv.enableLogging)
	if enableLogging == 1 {
		log.Warn().Msgf("[KV SERVER=%d] "+format, append([]interface{}{kv.me}, a...)...)
	}
	return
}

func (kv *KVServer) Error(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&kv.enableLogging)
	if enableLogging == 1 {
		log.Error().Msgf("[KV SERVER=%d] "+format, append([]interface{}{kv.me}, a...)...)
	}
	return
}

func (kv *KVServer) Fatal(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&kv.enableLogging)
	if enableLogging == 1 {
		log.Fatal().Msgf("[KV SERVER=%d] "+format, append([]interface{}{kv.me}, a...)...)
	}
	return
}

func (kv *KVServer) Panic(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&kv.enableLogging)
	if enableLogging == 1 {
		log.Panic().Msgf("[KV SERVER=%d] "+format, append([]interface{}{kv.me}, a...)...)
	}
	return
}
