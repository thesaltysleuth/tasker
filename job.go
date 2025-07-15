package tasker

import "context"

type Handler func(ctx context.Context, args map[string]any) error

var registry = make(map[string]Handler)

func RegisterTask(name string, h Handler) { registry[name] = h }
