package domain

type Publisher interface {
    Publish(Message) error
}
