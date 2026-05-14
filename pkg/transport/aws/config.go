// Package aws holds shared AWS SDK client configuration consumed by the
// per-service subpackages (sns, sqs, …). The SDK package is aliased as
// `awssdk` inside our files to avoid colliding with this package's name.
package aws

import (
	"context"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
)

// maxRetryAttempts is the maximum number of SDK retryer attempts.
// Covers ThrottlingException, ProvisionedThroughputExceededException,
// RequestLimitExceeded, TooManyRequestsException, network errors and 5xx,
// with exponential backoff, jitter, and adaptive client-side rate limiting.
const maxRetryAttempts = 8

// NewConfig loads the default AWS configuration with adaptive retry.
//
// Region resolution follows the SDK's standard chain (AWS_REGION env var,
// shared config file, etc.); us-east-1 is the fallback when nothing is set.
func NewConfig() (*awssdk.Config, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithDefaultRegion("us-east-1"),
		config.WithRetryer(func() awssdk.Retryer {
			return retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
				o.StandardOptions = append(o.StandardOptions, func(so *retry.StandardOptions) {
					so.MaxAttempts = maxRetryAttempts
				})
			})
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	return &cfg, nil
}
