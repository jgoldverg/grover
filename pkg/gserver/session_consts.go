package gserver

import (
	"errors"
	"time"
)

const (
	helloTimeout = 30 * time.Second
	defaultMTU   = 1000
)

var errNotRegularFile = errors.New("path is not a regular file")
