// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"errors"

	ec2_types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

var (
	ErrInstanceTypeNotFound     = errors.New("instance type not found")
	ErrAMINotFound              = errors.New("AMI not found")
	ErrSubnetsNotSet            = errors.New("aws-subnets must be set")
	ErrSubnetNotFound           = errors.New("subnet not found")
	ErrSecurityGroupNotFound    = errors.New("security group not found")
	ErrArchMismatch             = errors.New("instance type architecture not supported by AMI")
	ErrInstanceTypeArchitecture = errors.New("instance type must report exactly one architecture")
	ErrTypeNotInRegion          = errors.New("instance type not offered in region")
	ErrNoDeployCandidates       = errors.New("no deploy candidates resolved")
	ErrNoMatchingCandidate      = errors.New("no deploy candidate matches requested capability")
	ErrUnknownArchitecture      = errors.New("unknown architecture")
	ErrRegionNotSet             = errors.New("aws-region must be set for unqualified values")
	ErrReservedTagPrefix        = errors.New("reserved tag prefix")
)

// regionConfig contains the resources that exist together in an AWS region.
type regionConfig struct {
	region         string
	image          ec2_types.Image
	subnets        []string
	securityGroups []string
}

// deployCandidate is one deploy specification the provider tries in order.
// It owns its instance type and all region-scoped resources it needs.
type deployCandidate struct {
	instanceType ec2_types.InstanceTypeInfo
	regionConfig regionConfig
}
