//go:build !linux

package cgroup

import "log/slog"

// System — вне Linux cgroup нет.
func System(*slog.Logger) (*Manager, error) { return nil, ErrUnavailable }
