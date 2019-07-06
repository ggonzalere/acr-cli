// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package api

import (
	"context"

	acrapi "github.com/Azure/acr-cli/acr"
)

// ACRClient contains the methods that will be used to call the autorest generated SDK
type ACRClient interface {
	AcrListTags(ctx context.Context, loginURL string, auth string, repoName string, orderBy string, last string) (*acrapi.TagAttributeList, error)
	AcrDeleteTag(ctx context.Context, loginURL string, auth string, repoName string, reference string) error
	AcrListManifests(ctx context.Context, loginURL string, auth string, repoName string, orderBy string, last string) (*acrapi.ManifestAttributeList, error)
	DeleteManifest(ctx context.Context, loginURL string, auth string, repoName string, reference string) error
}
