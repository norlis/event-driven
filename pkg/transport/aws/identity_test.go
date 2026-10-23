package aws

import (
	"context"
	"errors"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
)

type fakeSTS struct {
	calls int
	err   error
	arn   string
}

func (f *fakeSTS) GetCallerIdentity(context.Context, *awssts.GetCallerIdentityInput, ...func(*awssts.Options)) (*awssts.GetCallerIdentityOutput, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &awssts.GetCallerIdentityOutput{Account: awssdk.String("123456789012"), Arn: awssdk.String(f.arn)}, nil
}

func TestIdentityRetriesAfterErrorAndCachesSuccess(t *testing.T) {
	t.Parallel()
	sts := &fakeSTS{err: errors.New("timeout"), arn: "arn:aws:sts::123456789012:assumed-role/app/x"}
	id := &Identity{Client: sts}

	if _, err := id.AccountID(t.Context()); err == nil {
		t.Fatal("first lookup succeeded despite STS error")
	}
	sts.err = nil
	acct, err := id.AccountID(t.Context())
	if err != nil || acct != "123456789012" {
		t.Fatalf("second lookup = %q, %v; want account and nil", acct, err)
	}
	if _, err := id.Arn(t.Context()); err != nil {
		t.Fatal(err)
	}
	if sts.calls != 2 {
		t.Errorf("STS called %d times, want 2 (one failure, one cached success)", sts.calls)
	}
}

func TestIdentityPartition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		arn, want string
		wantErr   bool
	}{
		{arn: "arn:aws:sts::123456789012:assumed-role/app/x", want: "aws"},
		{arn: "arn:aws-cn:sts::123456789012:user/x", want: "aws-cn"},
		{arn: "arn:aws-us-gov:iam::123456789012:root", want: "aws-us-gov"},
		{arn: "not-an-arn", wantErr: true},
		{arn: "arn::sts::1:x", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.arn, func(t *testing.T) {
			t.Parallel()
			got, err := (&Identity{Client: &fakeSTS{arn: tt.arn}}).Partition(t.Context())
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("Partition() = %q, %v; want %q, err=%v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}
