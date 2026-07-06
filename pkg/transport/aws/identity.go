package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
)

// CallerIdentityAPI is the STS subset Identity depends on. It exists so tests
// can inject a fake; production code leaves Client nil and uses the real STS.
type CallerIdentityAPI interface {
	GetCallerIdentity(ctx context.Context, in *awssts.GetCallerIdentityInput, optFns ...func(*awssts.Options)) (*awssts.GetCallerIdentityOutput, error)
}

// Identity exposes the AWS account ID, caller ARN and region tied to a given
// SDK config. Account ID and ARN come from a single STS GetCallerIdentity
// call on first use and are cached; the region is read from the SDK config
// (AWS_REGION env, shared config file, or IMDS in EC2/ECS/Lambda).
//
// Safe for concurrent use.
type Identity struct {
	// Config is the SDK config to resolve credentials and region from.
	Config *awssdk.Config

	// Client overrides the STS client (tests). Nil means built from Config.
	Client CallerIdentityAPI

	once      sync.Once
	accountID string
	arn       string
	err       error
}

// resolve performs the single cached GetCallerIdentity lookup.
func (i *Identity) resolve(ctx context.Context) error {
	if i.Config == nil && i.Client == nil {
		return errors.New("aws: Identity requires Config")
	}
	i.once.Do(func() {
		client := i.Client
		if client == nil {
			client = awssts.NewFromConfig(*i.Config)
		}
		out, err := client.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
		if err != nil {
			i.err = fmt.Errorf("aws: GetCallerIdentity: %w", err)
			return
		}
		i.accountID = awssdk.ToString(out.Account)
		i.arn = awssdk.ToString(out.Arn)
	})
	return i.err
}

// AccountID returns the AWS account ID of the caller. The STS lookup happens
// at most once per Identity instance.
func (i *Identity) AccountID(ctx context.Context) (string, error) {
	if err := i.resolve(ctx); err != nil {
		return "", err
	}
	return i.accountID, nil
}

// Arn returns the caller's ARN (e.g. arn:aws:sts::123:assumed-role/X/session),
// the canonical audit identifier of the principal. Same cached lookup as
// AccountID — asking for both costs one STS call.
func (i *Identity) Arn(ctx context.Context) (string, error) {
	if err := i.resolve(ctx); err != nil {
		return "", err
	}
	return i.arn, nil
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
