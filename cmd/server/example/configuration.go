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
	PublishTraceTopic       string `mapstructure:"publish-trace-topic"`
	SubscribeTrace          string `mapstructure:"subscribe-trace"`
}

func NewConfigurationExample() *Configuration {
	return &Configuration{
		LogLevel: "info",
		Messaging: Messaging{
			PublishDestinationTopic: "",
			SubscribeDestination:    os.Getenv("EVT_APP_SUBSCRIPTION"),
			PublishTraceTopic:       os.Getenv("EVT_PUBLISH_TRACE_TOPIC"),
			SubscribeTrace:          os.Getenv("EVT_TRACE_SUBSCRIPTION"),
		},
		Cloud: Cloud{GCloudProjectId: os.Getenv("GCP_PROJECT_ID")},
	}
}
