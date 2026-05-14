package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
)

// Identity exposes the AWS account ID and region tied to a given SDK config.
// The account ID is fetched lazily via STS GetCallerIdentity on first call
// and cached; the region is read from the SDK config (AWS_REGION env, shared
// config file, or IMDS in EC2/ECS/Lambda).
//
// Safe for concurrent use.
type Identity struct {
	// Config is the SDK config to resolve credentials and region from.
	Config *awssdk.Config

	once      sync.Once
	accountID string
	err       error
}

// AccountID returns the AWS account ID of the caller. The STS lookup happens
// at most once per Identity instance.
func (i *Identity) AccountID(ctx context.Context) (string, error) {
	if i.Config == nil {
		return "", errors.New("aws: Identity requires Config")
	}
	i.once.Do(func() {
		out, err := awssts.NewFromConfig(*i.Config).GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
		if err != nil {
			i.err = fmt.Errorf("aws: GetCallerIdentity: %w", err)
			return
		}
		i.accountID = awssdk.ToString(out.Account)
	})
	return i.accountID, i.err
}

// Region returns the region resolved by the SDK config.
func (i *Identity) Region() (string, error) {
	if i.Config == nil {
		return "", errors.New("aws: Identity requires Config")
	}
	if i.Config.Region == "" {
		return "", errors.New("aws: config has no region (set AWS_REGION or pass it via config option)")
	}
	return i.Config.Region, nil
}
