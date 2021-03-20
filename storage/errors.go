package storage

import "errors"

// ErrSizeLimitOutOfRange is returned when the given size is bigger than our limit
var ErrSizeLimitOutOfRange = errors.New("size limit is, for now, file = [256KB, 2MB], itemSize <= 128KB")

// ErrDataTooBig is returned if the given data is bigger than the predefined limit
var ErrDataTooBig = errors.New("data size is too big")

// ErrWalFileCorrupted is returned when checksum check fails
var ErrWalFileCorrupted = errors.New("wal file checksum mismatch, the file is considered corrupted")

// ErrWalManagerClosed is returned when `Record()` is called after Close is called
var ErrWalManagerClosed = errors.New("wal Manager is already closed")
