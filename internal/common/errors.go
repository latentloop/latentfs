package common

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrExists        = errors.New("already exists")
	ErrNotDir        = errors.New("not a directory")
	ErrIsDir         = errors.New("is a directory")
	ErrNotEmpty      = errors.New("directory not empty")
	ErrInvalidPath   = errors.New("invalid path")
	ErrInvalidHandle = errors.New("invalid handle")
	ErrReadOnly      = errors.New("read-only filesystem")
	ErrIO            = errors.New("I/O error")
)
