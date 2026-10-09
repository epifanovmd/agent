package message

import "time"

// Пределы и сроки (§16).
const (
	MaxMessageBytes      = 8 << 20
	MaxEnrollBytes       = 64 << 10
	MaxConfigBytes       = 4 << 20
	MaxEventDataBytes    = 64 << 10
	MaxMetricsBytes      = 1 << 20
	MaxHealthBytes       = 64 << 10
	MaxManifestBytes     = 64 << 10
	MaxFetchRequestBytes = 4 << 20
	MaxFetchBodyBytes    = 32 << 20
	MaxChunkBytes        = 64 << 10
	MaxActionResultBytes = 4 << 20
	MaxOutbox            = 10000
	StreamBacklog        = 600
	MaxFetches           = 64
	MaxLogBatch          = 500
	DefaultLogLines      = 200
	MaxLogLines          = 5000

	ProbeTimeout        = 2 * time.Second
	ConfigTimeout       = 30 * time.Second
	DefaultFetchTimeout = 30 * time.Second
	MaxFetchTimeout     = 10 * time.Minute
	WorkerStartTimeout  = 60 * time.Second
	UpdateHealthTimeout = time.Minute
	HealthInterval      = 10 * time.Second
	HealthMisses        = 3
	MinWatchInterval    = time.Second
	ConfigRetry         = 25 * time.Second
	LogBatchInterval    = time.Second
	HelloTimeout        = 10 * time.Second
	WelcomeTimeout      = 15 * time.Second
	PingInterval        = 20 * time.Second
	PongTimeout         = 10 * time.Second
)
