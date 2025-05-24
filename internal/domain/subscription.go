package domain

import "context"

type Subscription interface {
	Start(ctx context.Context, handler func(Message)) error
}
