/*
Copyright (c) 2017 [Rancher Labs, Inc.](http://rancher.com), Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Adapted from Kubernetes gc manager. See https://github.com/kubernetes/kubernetes
*/

package imagegc

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/shirou/gopsutil/disk"
	"golang.org/x/net/context"
)

type GCManager interface {
	// GarbageCollect can be called in a go-routine to delete unused image
	GarbageCollect() error

	// Start sync up the images with detected time and lastUsedTime
	Start() error
}

type containerRuntime interface {
	ContainerList(ctx context.Context, options types.ContainerListOptions) ([]types.Container, error)

	ImageInspectWithRaw(ctx context.Context, imageID string) (types.ImageInspect, []byte, error)

	ImageList(ctx context.Context, options types.ImageListOptions) ([]types.ImageSummary, error)

	ImageRemove(ctx context.Context, imageID string, options types.ImageRemoveOptions) ([]types.ImageDelete, error)
}

type diskCollector interface {
	DiskUsage() (*disk.UsageStat, error)
}

type diskCollectorImpl struct{}

func (d diskCollectorImpl) DiskUsage() (*disk.UsageStat, error) {
	return disk.Usage(".")
}

type GCPolicy struct {
	// Any usage above this threshold will always trigger garbage collection.
	// This is the highest usage we will allow.
	HighThresholdPercent float64

	// Any usage below this threshold will never trigger garbage collection.
	// This is the lowest threshold we will try to garbage collect to.
	LowThresholdPercent float64

	// Minimum age at which an image can be garbage collected.
	MinAge time.Duration

	// GC interval
	GCInterval time.Duration
}

type imageRecord struct {
	firstDetectedTime time.Time

	lastUsedTime time.Time

	size int64
}

type imageGCManagerImpl struct {
	policy *GCPolicy

	imageRecords map[string]*imageRecord

	imageRecordsLock sync.Mutex

	initialized bool

	runtime containerRuntime

	collector diskCollector
}

func NewImageManagerImpl(policy *GCPolicy, client *client.Client) (GCManager, error) {
	// Validate policy.
	if policy.HighThresholdPercent < 0 || policy.HighThresholdPercent > 100 {
		return nil, fmt.Errorf("invalid HighThresholdPercent %v, must be in range [0-100]", policy.HighThresholdPercent)
	}
	if policy.LowThresholdPercent < 0 || policy.LowThresholdPercent > 100 {
		return nil, fmt.Errorf("invalid LowThresholdPercent %v, must be in range [0-100]", policy.LowThresholdPercent)
	}
	if policy.LowThresholdPercent > policy.HighThresholdPercent {
		return nil, fmt.Errorf("LowThresholdPercent %v can not be higher than HighThresholdPercent %v", policy.LowThresholdPercent, policy.HighThresholdPercent)
	}
	im := &imageGCManagerImpl{
		policy:           policy,
		imageRecords:     make(map[string]*imageRecord),
		imageRecordsLock: sync.Mutex{},
		initialized:      false,
		runtime:          client,
		collector:        diskCollectorImpl{},
	}
	return im, nil
}

func (im *imageGCManagerImpl) Start() error {
	go func() {
		for {
			var ts time.Time
			if im.initialized {
				ts = time.Now()
			}
			err := im.detectImage(ts)
			if err != nil {
				logrus.Errorf("[ImageGCmanager] Failed to detect images: %v", err)
			} else {
				im.initialized = true
			}
			time.Sleep(time.Minute * 5)
		}
	}()

	return nil
}

func (im *imageGCManagerImpl) detectImage(ts time.Time) error {
	images, err := im.runtime.ImageList(context.Background(), types.ImageListOptions{})
	if err != nil {
		return err
	}
	containers, err := im.runtime.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return err
	}

	imageInUse := map[string]struct{}{}
	for _, container := range containers {
		logrus.Debugf("Container %v(%v) is using images %v(%v)", container.Names, container.ID, container.Image, container.ImageID)
		imageInUse[container.ImageID] = struct{}{}
	}

	currentImages := map[string]struct{}{}
	now := time.Now()

	im.imageRecordsLock.Lock()
	defer im.imageRecordsLock.Unlock()
	for _, image := range images {
		logrus.Debugf("Adding image ID %v to currentImages", image.ID)
		currentImages[image.ID] = struct{}{}

		if _, ok := im.imageRecords[image.ID]; !ok {
			logrus.Debugf("Image ID %s is new", image.ID)
			im.imageRecords[image.ID] = &imageRecord{
				firstDetectedTime: ts,
			}
		}

		if isImageUsed(image, imageInUse) {
			logrus.Debugf("Setting Image ID %s lastUsed to %v", image.ID, now)
			im.imageRecords[image.ID].lastUsedTime = now
		}
		logrus.Debugf("Image ID %s has size %d", image.ID, image.Size)
		im.imageRecords[image.ID].size = image.Size
	}

	// Remove old images from our records.
	for image := range im.imageRecords {
		if _, ok := currentImages[image]; !ok {
			logrus.Debugf("Image ID %s is no longer present; removing from imageRecords", image)
			delete(im.imageRecords, image)
		}
	}
	return nil
}

func (im *imageGCManagerImpl) GarbageCollect() error {
	usage, err := im.collector.DiskUsage()
	if err != nil {
		return err
	}
	percentage := usage.UsedPercent
	if percentage >= im.policy.HighThresholdPercent {
		amountToFree := usage.Total*uint64(100.00-im.policy.LowThresholdPercent)/100 - usage.Free
		logrus.Infof("[imageGCManager]: Disk usage is at %v%% which is over the high threshold (%v%%). Trying to free %v bytes", percentage, im.policy.HighThresholdPercent, amountToFree)
		freed, err := im.freeSpace(amountToFree, time.Now())
		if err != nil {
			return err
		}

		if freed < amountToFree {
			return fmt.Errorf("failed to garbage collect required amount of images. Wanted to free %d, but freed %d", amountToFree, freed)
		}
	}
	return nil
}

func (im *imageGCManagerImpl) freeSpace(amountToFree uint64, ts time.Time) (uint64, error) {
	err := im.detectImage(ts)
	if err != nil {
		return 0, err
	}

	im.imageRecordsLock.Lock()
	defer im.imageRecordsLock.Unlock()

	// Get all images in eviction order.
	images := make([]evictionInfo, 0, len(im.imageRecords))
	for image, record := range im.imageRecords {
		images = append(images, evictionInfo{
			id:          image,
			imageRecord: *record,
		})
	}
	sort.Sort(byLastUsedAndDetected(images))

	var deletionErrors []error
	spaceFreed := int64(0)
	for _, image := range images {
		logrus.Debugf("Evaluating image ID %s for possible garbage collection", image.id)
		// Images that are currently in used were given a newer lastUsed.
		if image.lastUsedTime.Equal(ts) || image.lastUsedTime.After(ts) {
			logrus.Debugf("Image ID %s has lastUsed=%v which is >= freeTime=%v, not eligible for garbage collection", image.id, image.lastUsedTime, ts)
			break
		}

		// Avoid garbage collect the image if the image is not old enough.
		// In such a case, the image may have just been pulled down, and will be used by a container right away.

		if ts.Sub(image.firstDetectedTime) < im.policy.MinAge {
			logrus.Debugf("Image ID %s has age %v which is less than the policy's minAge of %v, not eligible for garbage collection", image.id, ts.Sub(image.firstDetectedTime), im.policy.MinAge)
			continue
		}

		// Remove image. Continue despite errors.
		logrus.Infof("[imageGCManager]: Removing image %q to free %d bytes", image.id, image.size)
		err := im.deleteImage(image.id)
		if err != nil {
			deletionErrors = append(deletionErrors, err)
			continue
		}
		delete(im.imageRecords, image.id)
		spaceFreed += image.size

		if uint64(spaceFreed) >= amountToFree {
			break
		}
	}

	if len(deletionErrors) > 0 {
		return uint64(spaceFreed), fmt.Errorf("wanted to free %d, but freed %d space with errors in image deletion: %v", amountToFree, spaceFreed, deletionErrors)
	}
	return uint64(spaceFreed), nil
}

func (im *imageGCManagerImpl) deleteImage(imageID string) error {
	imageInspect, _, err := im.runtime.ImageInspectWithRaw(context.Background(), imageID)
	if err != nil {
		return err
	}
	if len(imageInspect.RepoTags) > 1 {
		for _, tag := range imageInspect.RepoTags {
			if _, err := im.runtime.ImageRemove(context.Background(), tag, types.ImageRemoveOptions{PruneChildren: true}); err != nil {
				return err
			}
		}
		return nil
	}
	_, err = im.runtime.ImageRemove(context.Background(), imageID, types.ImageRemoveOptions{PruneChildren: true})
	return nil
}

type evictionInfo struct {
	id string
	imageRecord
}

type byLastUsedAndDetected []evictionInfo

func (ev byLastUsedAndDetected) Len() int      { return len(ev) }
func (ev byLastUsedAndDetected) Swap(i, j int) { ev[i], ev[j] = ev[j], ev[i] }
func (ev byLastUsedAndDetected) Less(i, j int) bool {
	// Sort by last used, break ties by detected.
	if ev[i].lastUsedTime.Equal(ev[j].lastUsedTime) {
		return ev[i].firstDetectedTime.Before(ev[j].firstDetectedTime)
	}
	return ev[i].lastUsedTime.Before(ev[j].lastUsedTime)
}

func isImageUsed(image types.ImageSummary, imagesInUse map[string]struct{}) bool {
	// Check the image ID.
	if _, ok := imagesInUse[image.ID]; ok {
		return true
	}
	return false
}
