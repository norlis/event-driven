package pubsub

import (
	"context"
	"event-router/internal/domain"

	"cloud.google.com/go/pubsub"
)

type gcpPublisher struct {
	topicID string
}

func NewPublisher(topicID string) *gcpPublisher {
	return &gcpPublisher{topicID: topicID}
}

func (p *gcpPublisher) Publish(msg domain.Message) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "test-project")
	if err != nil {
		return err
	}

	topic := client.Topic(p.topicID)
	result := topic.Publish(ctx, &pubsub.Message{
		Data:       msg.Payload,
		Attributes: msg.Metadata,
	})

	_, err = result.Get(ctx)
	return err
}
