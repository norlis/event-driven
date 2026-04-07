package example

import "os"

type Configuration struct {
	Messaging Messaging `mapstructure:"messaging"`
	Cloud     Cloud     `mapstructure:"cloud"`
	LogLevel  string    `mapstructure:"level"`
}

type Cloud struct {
	GCloudProjectId string `mapstructure:"gcp-project-id"`
}

type Messaging struct {
	PublishDestinationTopic string `mapstructure:"publish-destination-topic"`
	SubscribeDestination    string `mapstructure:"subscribe-destination"`
}

func NewConfigurationExample() *Configuration {
	return &Configuration{
		LogLevel: "info",
		Messaging: Messaging{
			PublishDestinationTopic: os.Getenv("EVT_PUBLISH"),
			SubscribeDestination:    os.Getenv("EVT_SUBSCRIPTION"),
		},
		Cloud: Cloud{GCloudProjectId: os.Getenv("GCP_PROJECT_ID")},
	}
}
