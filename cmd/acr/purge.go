// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Azure/acr-cli/acr"
	"github.com/Azure/acr-cli/cmd/api"
	"github.com/Azure/acr-cli/cmd/worker"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// The constants for this file are defined here.
const (
	newPurgeCmdLongMessage = `acr purge: untag old images and delete dangling manifests.`
	purgeExampleMessage    = `  - Delete all tags that are older than 1 day
    acr purge -r MyRegistry --filter "MyRepository:^.*" --ago 1d

  - Delete all tags that are older than 1 day and begin with hello
    acr purge -r MyRegistry --filter "MyRepository:^hello.*" --ago 1d 

  - Delete all tags that match a regex filter and remove the dangling manifests
	acr purge -r MyRegistry --filter "MyRepository:RegexFilter" --ago 1d --untagged 
`

	defaultNumWorkers       = 6
	manifestListContentType = "application/vnd.docker.distribution.manifest.list.v2+json"
)

// purgeParameters defines the parameters that the purge command uses (including the registry name, username and password).
type purgeParameters struct {
	*rootParameters
	ago        string
	filters    []string
	untagged   bool
	dryRun     bool
	numWorkers int
}

// The WaitGroup is used to make sure that the http requests are finished before exiting the program, and also to limit the
// amount of concurrent http calls to the defaultNumWorkers
var wg sync.WaitGroup

// newPurgeCmd defines the purge command.
func newPurgeCmd(out io.Writer, rootParams *rootParameters) *cobra.Command {
	purgeParams := purgeParameters{rootParameters: rootParams}
	cmd := &cobra.Command{
		Use:     "purge",
		Short:   "Delete images from a registry.",
		Long:    newPurgeCmdLongMessage,
		Example: purgeExampleMessage,
		RunE: func(cmd *cobra.Command, args []string) error {
			// This context is used for all the http requests.
			ctx := context.Background()
			registryName, err := purgeParams.GetRegistryName()
			if err != nil {
				return err
			}
			loginURL := api.LoginURL(registryName)
			// An acrClient with authentication is generated, if the authentication cannot be resolved an error is returned.
			acrClient, err := api.GetAcrCLIClientWithAuth(loginURL, purgeParams.username, purgeParams.password, purgeParams.configs)
			if err != nil {
				return err
			}
			// In order to only have a fixed amount of http requests a dispatcher is started that will keep forwarding the jobs
			// to the workers, which are goroutines that continuously fetch for tags/manifests to delete.
			worker.StartDispatcher(ctx, &wg, acrClient, purgeParams.numWorkers)
			// A map is used to keep the regex tags for every repository.
			tagFilters := map[string][]string{}
			for _, filter := range purgeParams.filters {
				repoName, tagRegex, err := getRepositoryAndTagRegex(filter)
				if err != nil {
					return err
				}
				if _, ok := tagFilters[repoName]; ok {
					tagFilters[repoName] = append(tagFilters[repoName], tagRegex)
				} else {
					tagFilters[repoName] = []string{tagRegex}
				}
			}

			// In order to print a summary of the deleted tags/manifests the counters get updated everytime a repo is purged.
			deletedTagsCount := 0
			deletedManifestsCount := 0
			for repoName, listOfTagRegex := range tagFilters {
				tagRegex := listOfTagRegex[0]
				for i := 1; i < len(listOfTagRegex); i++ {
					// To only iterate through a repo once a big regex filter is made of all the filters of a particular repo.
					tagRegex = tagRegex + "|" + listOfTagRegex[i]
				}
				if !purgeParams.dryRun {
					// If this is not a dry-run the PurgeTags method is called, this method will actually delete tags with the help
					// of the dispatcher and the workers.
					singleDeletedTagsCount, err := PurgeTags(ctx, acrClient, loginURL, repoName, purgeParams.ago, tagRegex)
					if err != nil {
						return errors.Wrap(err, "failed to purge tags")
					}
					singleDeletedManifestsCount := 0
					// If the untagged flag is set then also manifests are deleted.
					if purgeParams.untagged {
						deletedManifestsCount, err = PurgeDanglingManifests(ctx, acrClient, loginURL, repoName)
						if err != nil {
							return errors.Wrap(err, "failed to purge manifests")
						}
					}
					// After every repository is purged the counters are updated.
					deletedTagsCount += singleDeletedTagsCount
					deletedManifestsCount += singleDeletedManifestsCount
				} else {
					// If this is a dry-run then no tag or manifests get deleted, but the counters still get updated.
					singleDeletedTagsCount, singleDeletedManifestsCount, err := DryRunPurge(ctx, acrClient, loginURL, repoName, purgeParams.ago, tagRegex, purgeParams.untagged)
					if err != nil {
						return err
					}
					deletedTagsCount += singleDeletedTagsCount
					deletedManifestsCount += singleDeletedManifestsCount

				}
			}
			// After all repos have been purged the summary is printed.
			fmt.Printf("\nNumber of deleted tags: %d\n", deletedTagsCount)
			fmt.Printf("Number of deleted manifests: %d\n", deletedManifestsCount)

			return nil
		},
	}

	cmd.Flags().BoolVar(&purgeParams.untagged, "untagged", false, "If untagged is set all manifest that do not have any tags associated to them will be deleted")
	cmd.Flags().BoolVar(&purgeParams.dryRun, "dry-run", false, "Don't actually remove any tag or manifest, instead, show if they would be deleted")
	cmd.Flags().IntVar(&purgeParams.numWorkers, "concurrency", defaultNumWorkers, "The number of concurrent requests sent to the registry")
	cmd.Flags().StringVar(&purgeParams.ago, "ago", "", "The images that were created before this duration will be deleted")
	cmd.Flags().StringArrayVarP(&purgeParams.filters, "filter", "f", nil, "Given as a regular expression, if a tag matches the pattern and is older than the time specified in ago it gets deleted")
	cmd.Flags().StringArrayVarP(&purgeParams.configs, "config", "c", nil, "auth config paths")
	// The filter flag is required because if it is not then the repository is specified there.
	cmd.MarkFlagRequired("filter")
	cmd.MarkFlagRequired("ago")
	return cmd
}

// getRepositoryAndTagRegex will just separate the filters that have the form <repository>:<regex filter>
func getRepositoryAndTagRegex(filter string) (string, string, error) {
	repoAndRegex := strings.Split(filter, ":")
	if len(repoAndRegex) != 2 {
		return "", "", errors.New("unable to correctly parse filter flag")
	}
	return repoAndRegex[0], repoAndRegex[1], nil
}

// PurgeTags deletes all tags that are older than the ago value and that match the tagFilter string.
func PurgeTags(ctx context.Context, acrClient api.AcrCLIClientInterface, loginURL string, repoName string, ago string, tagFilter string) (int, error) {
	fmt.Printf("Deleting tags for repository: %s\n", repoName)
	deletedTagsCount := 0
	agoDuration, err := ParseDuration(ago)
	if err != nil {
		return -1, err
	}
	timeToCompare := time.Now().UTC()
	// Since the ParseDuration returns a negative duration, it is added to the current duration in order to be able to easily compare
	// with the LastUpdatedTime attribute a tag has.
	timeToCompare = timeToCompare.Add(agoDuration)
	tagRegex, err := regexp.Compile(tagFilter)
	if err != nil {
		return -1, err
	}
	lastTag := ""
	tagsToDelete, lastTag, err := GetTagsToDelete(ctx, acrClient, repoName, tagRegex, timeToCompare, "")
	if err != nil {
		return -1, err
	}
	// GetTagsToDelete will return an empty lastTag when there are no more tags.
	for len(lastTag) > 0 {
		for _, tag := range *tagsToDelete {
			wg.Add(1)
			// The purge job is queued
			worker.QueuePurgeTag(loginURL, repoName, *tag.Name, *tag.Digest)
			deletedTagsCount++
		}
		// To not overflow the error channel capacity the PurgeTags method waits for a whole block of
		// 100 jobs to be finished before continuing.
		wg.Wait()
		for len(worker.ErrorChannel) > 0 {
			wErr := <-worker.ErrorChannel
			if wErr.Error != nil {
				return -1, wErr.Error
			}
		}
		tagsToDelete, lastTag, err = GetTagsToDelete(ctx, acrClient, repoName, tagRegex, timeToCompare, lastTag)
		if err != nil {
			return -1, err
		}
	}
	return deletedTagsCount, nil
}

// ParseDuration analog to time.ParseDuration() but with days added.
func ParseDuration(ago string) (time.Duration, error) {
	var days int
	var durationString string
	// The supported format is %d%s where the string is a valid go duration string.
	if strings.Contains(ago, "d") {
		if _, err := fmt.Sscanf(ago, "%dd%s", &days, &durationString); err != nil {
			fmt.Sscanf(ago, "%dd", &days)
			durationString = ""
		}
	} else {
		days = 0
		if _, err := fmt.Sscanf(ago, "%s", &durationString); err != nil {
			return time.Duration(0), err
		}
	}
	// The number of days gets converted to hours.
	duration := time.Duration(days) * 24 * time.Hour
	if len(durationString) > 0 {
		agoDuration, err := time.ParseDuration(durationString)
		if err != nil {
			return time.Duration(0), err
		}
		duration = duration + agoDuration
	}
	return (-1 * duration), nil
}

// GetTagsToDelete gets all tags that should be deleted according to the ago flag and the filter flag, this will at most return 100 tags,
// returns a pointer to a slice that contains the tags that will be deleted, the last tag obtained through the AcrListTags method
// and an error in case it occurred, the fourth return value contains a map that is used to determine how many tags a manifest has
func GetTagsToDelete(ctx context.Context,
	acrClient api.AcrCLIClientInterface,
	repoName string,
	filter *regexp.Regexp,
	timeToCompare time.Time,
	lastTag string) (*[]acr.TagAttributesBase, string, error) {

	var matches bool
	var lastUpdateTime time.Time
	resultTags, err := acrClient.GetAcrTags(ctx, repoName, "", lastTag)
	if err != nil {
		if resultTags != nil && resultTags.StatusCode == http.StatusNotFound {
			fmt.Printf("%s repository not found\n", repoName)
			return nil, "", nil
		}
		// An empty lastTag string is returned so there will not be any tag purged.
		return nil, "", err
	}
	newLastTag := ""
	if resultTags != nil && resultTags.TagsAttributes != nil && len(*resultTags.TagsAttributes) > 0 {
		tags := *resultTags.TagsAttributes
		tagsToDelete := []acr.TagAttributesBase{}
		for _, tag := range tags {
			matches = filter.MatchString(*tag.Name)
			if !matches {
				// If a tag does not match the regex then it not added to the list no matter the LastUpdateTime
				continue
			}
			lastUpdateTime, err = time.Parse(time.RFC3339Nano, *tag.LastUpdateTime)
			if err != nil {
				return nil, "", err
			}
			// If a tag did match the regex filter, is older than the specified duration and can be deleted then it is returned
			// as a tag to delete.
			if lastUpdateTime.Before(timeToCompare) && *(*tag.ChangeableAttributes).DeleteEnabled {
				tagsToDelete = append(tagsToDelete, tag)
			}
		}
		// The lastTag is updated to keep the for loop going.
		newLastTag = *tags[len(tags)-1].Name
		return &tagsToDelete, newLastTag, nil
	}
	// In case there are no more tags return empty string as lastTag so that the PurgeTags method stops
	return nil, "", nil
}

// PurgeDanglingManifests deletes all manifests that do not have any tags associated with them.
func PurgeDanglingManifests(ctx context.Context, acrClient api.AcrCLIClientInterface, loginURL string, repoName string) (int, error) {
	fmt.Printf("Deleting manifests for repository: %s\n", repoName)
	deletedManifestsCount := 0
	// Contrary to GetTagsToDelete, GetManifestsToDelete gets all the Manifests at once, this was done because if there is a manifest that has no
	// tag but is referenced by a multiarch manifest that has tags then it should not be deleted.
	manifestsToDelete, err := GetManifestsToDelete(ctx, acrClient, repoName)
	if err != nil {
		return -1, err
	}
	i := 0
	for _, manifest := range *manifestsToDelete {
		wg.Add(1)
		worker.QueuePurgeManifest(loginURL, repoName, *manifest.Digest)
		deletedManifestsCount++
		// Because the worker ErrorChannel has a capacity of 100 if has to periodically be checked
		if math.Mod(float64(i), 100) == 0 {
			wg.Wait()
			for len(worker.ErrorChannel) > 0 {
				wErr := <-worker.ErrorChannel
				if wErr.Error != nil {
					return -1, wErr.Error
				}
			}
		}
		i++
	}
	// Wait for all the worker jobs to finish.
	wg.Wait()
	for len(worker.ErrorChannel) > 0 {
		wErr := <-worker.ErrorChannel
		if wErr.Error != nil {
			return -1, wErr.Error
		}
	}
	return deletedManifestsCount, nil
}

// GetManifestsToDelete gets all the manifests that should be deleted, this means that do not have any tag and that do not form part
// of a manifest list that has tags refrerencing it.
func GetManifestsToDelete(ctx context.Context, acrClient api.AcrCLIClientInterface, repoName string) (*[]acr.ManifestAttributesBase, error) {
	lastManifestDigest := ""
	manifestsToDelete := []acr.ManifestAttributesBase{}
	resultManifests, err := acrClient.GetAcrManifests(ctx, repoName, "", lastManifestDigest)
	if err != nil {
		if resultManifests != nil && resultManifests.StatusCode == http.StatusNotFound {
			fmt.Printf("%s repository not found\n", repoName)
			return &manifestsToDelete, nil
		}
		return nil, err
	}
	// This will act as a set if a key is present then it should not be deleted because it is referenced by a multiarch manifest
	// that will not be deleted
	doNotDelete := map[string]bool{}
	candidatesToDelete := []acr.ManifestAttributesBase{}
	// Iterate over all manifests to discover multiarchitecture manifests
	for resultManifests != nil && resultManifests.ManifestsAttributes != nil {
		manifests := *resultManifests.ManifestsAttributes
		for _, manifest := range manifests {
			if *manifest.MediaType == manifestListContentType && manifest.Tags != nil {
				// If a manifest list is found and it has tags then all the dependentDigests are
				// marked to not be deleted.
				var manifestList []byte
				manifestList, err = acrClient.GetManifest(ctx, repoName, *manifest.Digest)
				if err != nil {
					return nil, err
				}
				var multiArchManifest MultiArchManifest
				err = json.Unmarshal(manifestList, &multiArchManifest)
				if err != nil {
					return nil, err
				}
				for _, dependentDigest := range multiArchManifest.Manifests {
					doNotDelete[dependentDigest.Digest] = true
				}
			} else if manifest.Tags == nil {
				// If the manifest has no tags left it is a candidate for deletion
				candidatesToDelete = append(candidatesToDelete, manifest)
			}
		}
		lastManifestDigest = *manifests[len(manifests)-1].Digest
		resultManifests, err = acrClient.GetAcrManifests(ctx, repoName, "", lastManifestDigest)
		if err != nil {
			return nil, err
		}
	}
	// Remove all manifests that should not be deleted
	for i := 0; i < len(candidatesToDelete); i++ {
		if _, ok := doNotDelete[*candidatesToDelete[i].Digest]; !ok {
			// if a manifest has no tags, is not part of a manifest list and can be deleted then it is added to the
			// manifestToDelete array.
			if *(*candidatesToDelete[i].ChangeableAttributes).DeleteEnabled {
				manifestsToDelete = append(manifestsToDelete, candidatesToDelete[i])
			}
		}
	}
	return &manifestsToDelete, nil
}

// DryRunPurge outputs everything that would be deleted if the purge command was executed
func DryRunPurge(ctx context.Context, acrClient api.AcrCLIClientInterface, loginURL string, repoName string, ago string, filter string, untagged bool) (int, int, error) {
	deletedTagsCount := 0
	deletedManifestsCount := 0
	deletedTags := map[string]int{}
	fmt.Printf("Deleting tags for repository: %s\n", repoName)
	agoDuration, err := ParseDuration(ago)
	if err != nil {
		return -1, -1, err
	}
	timeToCompare := time.Now().UTC()
	timeToCompare = timeToCompare.Add(agoDuration)
	regex, err := regexp.Compile(filter)
	if err != nil {
		return -1, -1, err
	}
	lastTag := ""
	tagsToDelete, lastTag, err := GetTagsToDelete(ctx, acrClient, repoName, regex, timeToCompare, "")

	if err != nil {
		return -1, -1, err
	}
	for len(lastTag) > 0 {
		for _, tag := range *tagsToDelete {
			if _, exists := deletedTags[*tag.Digest]; exists {
				deletedTags[*tag.Digest]++
			} else {
				deletedTags[*tag.Digest] = 1
			}
			fmt.Printf("%s/%s:%s\n", loginURL, repoName, *tag.Name)
			deletedTagsCount++
		}
		tagsToDelete, lastTag, err = GetTagsToDelete(ctx, acrClient, repoName, regex, timeToCompare, lastTag)
		if err != nil {
			return -1, -1, err
		}
	}
	if untagged {
		fmt.Printf("Deleting manifests for repository: %s\n", repoName)
		countMap, err := CountTagsByManifest(ctx, acrClient, repoName)
		if err != nil {
			return -1, -1, err
		}
		lastManifestDigest := ""
		resultManifests, err := acrClient.GetAcrManifests(ctx, repoName, "", lastManifestDigest)
		if err != nil {
			if resultManifests != nil && resultManifests.StatusCode == http.StatusNotFound {
				fmt.Printf("%s repository not found\n", repoName)
				return 0, 0, nil
			}
			return -1, -1, err
		}
		// This will act as a set if a key is present then it should not be deleted because it is referenced by a multiarch manifest
		// that will not be deleted
		doNotDelete := map[string]bool{}
		candidatesToDelete := []acr.ManifestAttributesBase{}
		// Iterate over all manifests to discover multiarchitecture manifests
		for resultManifests != nil && resultManifests.ManifestsAttributes != nil {
			manifests := *resultManifests.ManifestsAttributes
			for _, manifest := range manifests {
				if *manifest.MediaType == manifestListContentType && (*countMap)[*manifest.Digest] != deletedTags[*manifest.Digest] {
					var manifestList []byte
					manifestList, err = acrClient.GetManifest(ctx, repoName, *manifest.Digest)
					if err != nil {
						return -1, -1, err
					}
					var multiArchManifest MultiArchManifest
					err = json.Unmarshal(manifestList, &multiArchManifest)
					if err != nil {
						return -1, -1, err
					}
					for _, dependentDigest := range multiArchManifest.Manifests {
						doNotDelete[dependentDigest.Digest] = true
					}
				} else if (*countMap)[*manifest.Digest] == deletedTags[*manifest.Digest] {
					// If the manifest has no tags left it is a candidate for deletion
					candidatesToDelete = append(candidatesToDelete, manifest)
				}
			}
			lastManifestDigest = *manifests[len(manifests)-1].Digest
			resultManifests, err = acrClient.GetAcrManifests(ctx, repoName, "", lastManifestDigest)
			if err != nil {
				return -1, -1, err
			}
		}
		// Just print manifests that should be deleted.
		for i := 0; i < len(candidatesToDelete); i++ {
			if _, ok := doNotDelete[*candidatesToDelete[i].Digest]; !ok {
				fmt.Printf("%s/%s@%s\n", loginURL, repoName, *candidatesToDelete[i].Digest)
				deletedManifestsCount++
			}
		}
	}

	return deletedTagsCount, deletedManifestsCount, nil
}

// CountTagsByManifest returns a map that for a given manifest digest contains the number of tags associated to it.
func CountTagsByManifest(ctx context.Context, acrClient api.AcrCLIClientInterface, repoName string) (*map[string]int, error) {
	countMap := map[string]int{}
	lastTag := ""
	resultTags, err := acrClient.GetAcrTags(ctx, repoName, "", lastTag)
	if err != nil {
		if resultTags != nil && resultTags.StatusCode == http.StatusNotFound {
			//Repository not found, will be handled in the GetAcrManifests call
			return nil, nil
		}
		return nil, err
	}
	for resultTags != nil && resultTags.TagsAttributes != nil {
		tags := *resultTags.TagsAttributes
		for _, tag := range tags {
			if _, exists := countMap[*tag.Digest]; exists {
				countMap[*tag.Digest]++
			} else {
				countMap[*tag.Digest] = 1
			}
		}

		lastTag = *tags[len(tags)-1].Name
		resultTags, err = acrClient.GetAcrTags(ctx, repoName, "", lastTag)
		if err != nil {
			return nil, err
		}
	}
	return &countMap, nil
}

// In order to parse the content of a mutliarch manifest string the following structs were defined.
type MultiArchManifest struct {
	Manifests     []Manifest `json:"manifests"`
	MediaType     string     `json:"mediaType"`
	SchemaVersion int        `json:"schemaVersion"`
}

type Manifest struct {
	Digest    string   `json:"digest"`
	MediaType string   `json:"mediaType"`
	Platform  Platform `json:"platform"`
	Size      int64    `json:"size"`
}

type Platform struct {
	Architecture string `json:"architecture"`
	Os           string `json:"os"`
}
