package edax

import (
	"context"
	"encoding/json"
	"fmt"
)

// TypedCommandHandler decodes an envelope before calling a typed domain handler.
func TypedCommandHandler[C any, R any](codec PayloadCodec, handler func(context.Context, C) (R, error)) (CommandHandler, error) {
	if codec == nil || handler == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "typed handler requires a codec and function"}
	}
	return func(ctx context.Context, message CommandMessage) (any, error) {
		value, err := codec.Unmarshal(Payload{Name: message.Name, Version: message.Version, Data: message.Data})
		if err != nil {
			return nil, err
		}
		command, ok := value.(C)
		if !ok {
			return nil, &Error{Code: CodeCodec, Message: fmt.Sprintf("command codec returned %T for %s", value, message.Name)}
		}
		return handler(ctx, command)
	}, nil
}

// ExecuteAs converts an executed or replayed result to the caller's result type.
func ExecuteAs[R any](ctx context.Context, bus *CommandBus, message CommandMessage) (result R, err error) {
	value, err := bus.Execute(ctx, message)
	if err != nil {
		return result, err
	}
	if typed, ok := value.(R); ok {
		return typed, nil
	}
	raw, err := json.Marshal(value)
	if err == nil {
		err = json.Unmarshal(raw, &result)
	}
	if err != nil {
		return result, WrapError(CodeCodec, "command result: "+err.Error(), err)
	}
	return result, nil
}
