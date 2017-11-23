package core

import "time"

const (
	// VERSION is the current version.
	VERSION = "1.0.0"

	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 10 * time.Millisecond

	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 1 * time.Second

	// VERSION is the current version.
	READ_BUFFER_SIZE = 1024
)
