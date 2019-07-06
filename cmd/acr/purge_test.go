// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Azure/acr-cli/acr"
	"github.com/Azure/acr-cli/cmd/mocks"
	"github.com/golang/mock/gomock"
)

func TestPurgeTags(t *testing.T) {
	auth := "Basic Zm9vOmZvb3Bhc3N3b3Jk"
	loginURL := "foo.azurecr.io"
	repo := "bar"
	EmptyListTagsResult := &acr.TagAttributeList{
		Registry:  &loginURL,
		ImageName: &repo,
		Tags:      nil,
	}
	lastUpdateTime := string(time.Now().Add(-15 * time.Minute).UTC().Format(time.RFC3339Nano)) //Creation time -15minutes from current time
	tagName := "latest"
	tag := acr.TagAttributesBase{
		Name:           &tagName,
		LastUpdateTime: &lastUpdateTime,
	}
	singleTag := []acr.TagAttributesBase{tag}
	OneTagsResult := &acr.TagAttributeList{
		Registry:  &loginURL,
		ImageName: &repo,
		Tags:      &singleTag,
	}
	tagName1 := "v1"
	tag1 := acr.TagAttributesBase{
		Name:           &tagName1,
		LastUpdateTime: &lastUpdateTime,
	}
	multipleTags := []acr.TagAttributesBase{tag1}
	tagName2 := "v2"
	tag2 := acr.TagAttributesBase{
		Name:           &tagName2,
		LastUpdateTime: &lastUpdateTime,
	}
	multipleTags = append(multipleTags, tag2)
	tagName3 := "v3"
	tag3 := acr.TagAttributesBase{
		Name:           &tagName3,
		LastUpdateTime: &lastUpdateTime,
	}
	multipleTags = append(multipleTags, tag3)
	tagName4 := "v4"
	tag4 := acr.TagAttributesBase{
		Name:           &tagName4,
		LastUpdateTime: &lastUpdateTime,
	}
	multipleTags = append(multipleTags, tag4)
	FourTagsResult := &acr.TagAttributeList{
		Registry:  &loginURL,
		ImageName: &repo,
		Tags:      &multipleTags,
	}
	// First test if repository is not known AcrListTags returns an error
	t.Run("RepositoryNotFound", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSdk := mocks.NewMockACRClient(mockCtrl)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "").Return(nil, errors.New("Repository not found")).Times(1)
		err := PurgeTags(context.Background(), loginURL, auth, repo, "1d", "", mockSdk)
		if err == nil {
			t.Fatal("Expected repository not known error")
		}
	})
	// Second test, if there are no singleTag on a registry no error should show and no other methods should be called.
	t.Run("EmptyRepository", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSdk := mocks.NewMockACRClient(mockCtrl)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "").Return(EmptyListTagsResult, nil).Times(1)
		err := PurgeTags(context.Background(), loginURL, auth, repo, "1d", "", mockSdk)
		if err != nil {
			t.Fatal("Unexpected error")
		}
	})
	// Third test only one tag and it should not be deleted (ago flag), ListTags should be called two times and no other calls should be made
	t.Run("NoDeletionAgo", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSdk := mocks.NewMockACRClient(mockCtrl)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "").Return(OneTagsResult, nil).Times(1)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "latest").Return(EmptyListTagsResult, nil).Times(1)
		err := PurgeTags(context.Background(), loginURL, auth, repo, "1d", "", mockSdk)
		if err != nil {
			t.Fatal("Unexpected error")
		}
	})
	// Fourth test only one tag and it should be deleted according to ago flag but it does not match a regex filter
	t.Run("NoDeletionFilter", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSdk := mocks.NewMockACRClient(mockCtrl)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "").Return(OneTagsResult, nil).Times(1)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "latest").Return(EmptyListTagsResult, nil).Times(1)
		err := PurgeTags(context.Background(), loginURL, auth, repo, "0m", "^hello.*", mockSdk)
		if err != nil {
			t.Fatal("Unexpected error")
		}
	})
	// Fifth test, only one tag it should be deleted
	t.Run("OneTagDeletion", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSdk := mocks.NewMockACRClient(mockCtrl)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "").Return(OneTagsResult, nil).Times(1)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "latest").Return(EmptyListTagsResult, nil).Times(1)
		mockSdk.EXPECT().AcrDeleteTag(context.Background(), loginURL, auth, repo, "latest").Return(nil).Times(1)
		err := PurgeTags(context.Background(), loginURL, auth, repo, "0m", "^la.*", mockSdk)
		if err != nil {
			t.Fatal("Unexpected error")
		}
	})
	// Sixth test, no filter, all singleTag should be deleted, 5 singleTag in total, separated into two ACRListTags calls.
	t.Run("FiveTagDeletion", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSdk := mocks.NewMockACRClient(mockCtrl)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "").Return(OneTagsResult, nil).Times(1)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "latest").Return(FourTagsResult, nil).Times(1)
		mockSdk.EXPECT().AcrListTags(context.Background(), loginURL, auth, repo, "", "v4").Return(EmptyListTagsResult, nil).Times(1)
		mockSdk.EXPECT().AcrDeleteTag(context.Background(), loginURL, auth, repo, "latest").Return(nil).Times(1)
		mockSdk.EXPECT().AcrDeleteTag(context.Background(), loginURL, auth, repo, "v1").Return(nil).Times(1)
		mockSdk.EXPECT().AcrDeleteTag(context.Background(), loginURL, auth, repo, "v2").Return(nil).Times(1)
		mockSdk.EXPECT().AcrDeleteTag(context.Background(), loginURL, auth, repo, "v3").Return(nil).Times(1)
		mockSdk.EXPECT().AcrDeleteTag(context.Background(), loginURL, auth, repo, "v4").Return(nil).Times(1)
		err := PurgeTags(context.Background(), loginURL, auth, repo, "0m", "", mockSdk)
		if err != nil {
			t.Fatal("Unexpected error")
		}
	})
	// Seventh test, invalid regex filter
	t.Run("InvalidRegex", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSdk := mocks.NewMockACRClient(mockCtrl)
		err := PurgeTags(context.Background(), loginURL, auth, repo, "1d", "[", mockSdk)
		if err == nil {
			t.Fatal("Expected invalid regex exception")
			return
		}
		if err.Error() != "error parsing regexp: missing closing ]: `[`" {
			t.Fatalf("Expected error parsing regexp: missing closing ]: `[`, got %s", err.Error())
		}
	})
	// TODO add operation not allowed test
}

func TestParseDuration(t *testing.T) {
	tables := []struct {
		durationString string
		duration       time.Duration
		err            error
	}{
		{"15m", -15 * time.Minute, nil},
		{"1d1h3m", -25*time.Hour - 3*time.Minute, nil},
		{"3d", -3 * 24 * time.Hour, nil},
		{"", 0, io.EOF},
		{"15p", 0, errors.New("time: unknown unit p in duration 15p")},
		{"15", 0 * time.Minute, errors.New("time: missing unit in duration 15")},
	}

	for _, table := range tables {
		durationResult, errorResult := ParseDuration(table.durationString)
		if durationResult != table.duration {
			t.Fatalf("ParseDuration of %s incorrect, expected %v got %v", table.durationString, table.duration, durationResult)
		}
		if errorResult != table.err {
			if errorResult.Error() != table.err.Error() {
				t.Fatalf("ParseDuration of %s incorrect, expected %v got %v", table.durationString, table.err, errorResult)
			}
		}
	}
}
