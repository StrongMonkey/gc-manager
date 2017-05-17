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
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/disk"
	"golang.org/x/net/context"
	"gopkg.in/check.v1"
)

const (
	testImageUUID = "daishan1992/echo-hello:latest"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	check.TestingT(t)
}

type ComputeTestSuite struct {
}

var _ = check.Suite(&ComputeTestSuite{})

func (s *ComputeTestSuite) SetUpSuite(c *check.C) {
}

type containerRuntimeFake struct {
	containerList []types.Container

	imageList []types.ImageSummary
}

func (c *containerRuntimeFake) ContainerList(ctx context.Context, options types.ContainerListOptions) ([]types.Container, error) {
	return c.containerList, nil
}

func (c *containerRuntimeFake) ImageInspectWithRaw(ctx context.Context, imageID string) (types.ImageInspect, []byte, error) {
	return types.ImageInspect{ID: imageID}, nil, nil
}

func (c *containerRuntimeFake) ImageList(ctx context.Context, options types.ImageListOptions) ([]types.ImageSummary, error) {
	return c.imageList, nil
}

func (c *containerRuntimeFake) ImageRemove(ctx context.Context, imageID string, options types.ImageRemoveOptions) ([]types.ImageDelete, error) {
	for index, image := range c.imageList {
		if image.ID == imageID {
			c.imageList = append(c.imageList[:index], c.imageList[index+1:]...)
		}
	}
	return nil, nil
}

type diskCollectorFake struct {
	diskStats *disk.UsageStat
}

func (d *diskCollectorFake) DiskUsage() (*disk.UsageStat, error) {
	return d.diskStats, nil
}

type failedDiskCollector struct{}

func (d *failedDiskCollector) DiskUsage() (*disk.UsageStat, error) {
	return nil, errors.New("fake error")
}

func newFakeImageGCManager(policy *GCPolicy) (*imageGCManagerImpl, *containerRuntimeFake, *diskCollectorFake) {
	fakeRuntime := &containerRuntimeFake{}
	diskFakeCollector := &diskCollectorFake{}
	im := &imageGCManagerImpl{
		policy:           policy,
		imageRecords:     make(map[string]*imageRecord),
		imageRecordsLock: sync.Mutex{},
		initialized:      true,
		runtime:          fakeRuntime,
		collector:        diskFakeCollector,
	}
	return im, fakeRuntime, diskFakeCollector
}

func (s *ComputeTestSuite) TestDetectImagesInitialDetect(c *check.C) {
	manager, fakeRuntime, _ := newFakeImageGCManager(&GCPolicy{})
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 100},
		{ID: "test-b", Size: 200},
		{ID: "test-c", Size: 300},
	}
	fakeRuntime.containerList = []types.Container{
		{ID: "test-a-con", ImageID: "test-a"},
		{ID: "test-b-con", ImageID: "test-b"},
	}
	var zero time.Time
	err := manager.detectImage(zero)
	c.Assert(err, check.IsNil)
	c.Assert(manager.imageRecords, check.HasLen, 3)
	c.Assert(manager.imageRecords["test-c"].firstDetectedTime, check.Equals, zero)
	c.Assert(manager.imageRecords["test-c"].lastUsedTime, check.Equals, zero)
	c.Assert(manager.imageRecords["test-c"].size, check.Equals, int64(300))

	c.Assert(manager.imageRecords["test-a"].firstDetectedTime, check.Equals, zero)
	c.Assert(manager.imageRecords["test-b"].firstDetectedTime, check.Equals, zero)
	c.Assert(manager.imageRecords["test-a"].lastUsedTime, check.Equals, manager.imageRecords["test-b"].lastUsedTime)
}

func (s *ComputeTestSuite) TestDetectNewImages(c *check.C) {
	manager, fakeRuntime, _ := newFakeImageGCManager(&GCPolicy{})
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 100},
		{ID: "test-b", Size: 200},
		{ID: "test-c", Size: 300},
	}
	var zero time.Time
	err := manager.detectImage(zero)
	c.Assert(err, check.IsNil)
	c.Assert(manager.imageRecords, check.HasLen, 3)
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 100},
		{ID: "test-b", Size: 200},
		{ID: "test-c", Size: 300},
		{ID: "test-d", Size: 400},
	}
	fakeRuntime.containerList = []types.Container{
		{ID: "test-d-con", ImageID: "test-d"},
	}
	detectedTime := zero.Add(time.Second)
	startTime := time.Now().Add(-time.Millisecond)
	err = manager.detectImage(detectedTime)
	c.Assert(err, check.IsNil)
	c.Assert(manager.imageRecords, check.HasLen, 4)
	c.Assert(manager.imageRecords["test-a"].firstDetectedTime, check.Equals, zero)
	c.Assert(manager.imageRecords["test-d"].firstDetectedTime, check.Equals, detectedTime)
	c.Assert(manager.imageRecords["test-d"].lastUsedTime.After(startTime), check.Equals, true)
	c.Assert(manager.imageRecords["test-d"].size, check.Equals, int64(400))
}

func (s *ComputeTestSuite) TestDetectImageWithRemoving(c *check.C) {
	manager, fakeRuntime, _ := newFakeImageGCManager(&GCPolicy{})
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 100},
		{ID: "test-b", Size: 200},
		{ID: "test-c", Size: 300},
	}
	var zero time.Time
	err := manager.detectImage(zero)
	c.Assert(err, check.IsNil)
	c.Assert(manager.imageRecords, check.HasLen, 3)

	fakeRuntime.imageList = []types.ImageSummary{}
	err = manager.detectImage(time.Now())
	c.Assert(manager.imageRecords, check.HasLen, 0)
}

func (s *ComputeTestSuite) TestFreeSpaceImagesInUseContainersAreIgnored(c *check.C) {
	manager, fakeRuntime, _ := newFakeImageGCManager(&GCPolicy{})
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 100},
		{ID: "test-b", Size: 200},
		{ID: "test-c", Size: 300},
	}
	fakeRuntime.containerList = []types.Container{
		{ID: "test-c-con", ImageID: "test-c"},
	}
	spaceFreed, err := manager.freeSpace(300, time.Now())
	c.Assert(err, check.IsNil)
	c.Assert(spaceFreed, check.Equals, uint64(300))
	c.Assert(fakeRuntime.imageList, check.HasLen, 1)
}

func (s *ComputeTestSuite) TestFreeSpaceRemoveByLeastRecentlyUsed(c *check.C) {
	manager, fakeRuntime, _ := newFakeImageGCManager(&GCPolicy{})
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 200},
		{ID: "test-b", Size: 200},
	}
	fakeRuntime.containerList = []types.Container{
		{ID: "test-a-con", ImageID: "test-a"},
		{ID: "test-b-con", ImageID: "test-b"},
	}
	var zero time.Time
	err := manager.detectImage(zero)
	c.Assert(err, check.IsNil)

	fakeRuntime.containerList = []types.Container{
		{ID: "test-a-con", ImageID: "test-a"},
	}
	err = manager.detectImage(time.Now())
	c.Assert(err, check.IsNil)

	fakeRuntime.containerList = []types.Container{}
	err = manager.detectImage(time.Now())
	c.Assert(err, check.IsNil)
	c.Assert(manager.imageRecords["test-a"].lastUsedTime.After(manager.imageRecords["test-b"].lastUsedTime), check.Equals, true)

	spaceFreed, err := manager.freeSpace(200, time.Now())
	c.Assert(err, check.IsNil)
	c.Assert(spaceFreed, check.Equals, uint64(200))
	c.Assert(fakeRuntime.imageList[0].ID, check.Equals, "test-a")
}

func (s *ComputeTestSuite) TestFreeSpaceTiesBrokenByDetectedTime(c *check.C) {
	manager, fakeRuntime, _ := newFakeImageGCManager(&GCPolicy{})
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 200},
	}
	var zero time.Time
	err := manager.detectImage(zero)
	c.Assert(err, check.IsNil)

	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 200},
		{ID: "test-b", Size: 200},
	}
	err = manager.detectImage(time.Now())
	c.Assert(err, check.IsNil)

	// make that a is detected before b, but two are used at the same time
	fakeRuntime.containerList = []types.Container{
		{ID: "test-a-con", ImageID: "test-a"},
		{ID: "test-b-con", ImageID: "test-b"},
	}
	err = manager.detectImage(time.Now())
	c.Assert(err, check.IsNil)

	fakeRuntime.containerList = []types.Container{}
	err = manager.detectImage(time.Now())
	c.Assert(err, check.IsNil)

	spaceFreed, err := manager.freeSpace(200, time.Now())
	c.Assert(err, check.IsNil)
	c.Assert(spaceFreed, check.Equals, uint64(200))
	c.Assert(fakeRuntime.imageList[0].ID, check.Equals, "test-b")
}

func (s *ComputeTestSuite) TestGarbageCollectBelowLowThreshold(c *check.C) {
	policy := &GCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	manager, _, mockDiskCollector := newFakeImageGCManager(policy)
	mockDiskCollector.diskStats = &disk.UsageStat{
		Total:       1000,
		Used:        600,
		UsedPercent: 60.0,
	}
	err := manager.GarbageCollect()
	c.Assert(err, check.IsNil)
}

func (s *ComputeTestSuite) TestGarbageCollectDiskCollectorFailure(c *check.C) {
	policy := &GCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	manager, _, _ := newFakeImageGCManager(policy)
	manager.collector = &failedDiskCollector{}

	err := manager.GarbageCollect()
	c.Assert(err, check.NotNil)
}

func (s *ComputeTestSuite) TestGarbageCollectBelowSuccess(c *check.C) {
	policy := &GCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	manager, fakeRuntime, mockDiskCollector := newFakeImageGCManager(policy)
	mockDiskCollector.diskStats = &disk.UsageStat{
		Total:       1000,
		Used:        950,
		UsedPercent: 95.0,
	}
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 200},
	}
	err := manager.GarbageCollect()
	c.Assert(err, check.IsNil)
	c.Assert(fakeRuntime.imageList, check.HasLen, 0)
}

func (s *ComputeTestSuite) TestGarbageCollectNotEnoughFreed(c *check.C) {
	policy := &GCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	manager, fakeRuntime, mockDiskCollector := newFakeImageGCManager(policy)
	mockDiskCollector.diskStats = &disk.UsageStat{
		Total:       1000,
		Used:        950,
		UsedPercent: 95.0,
	}
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 20},
	}
	err := manager.GarbageCollect()
	c.Assert(err, check.NotNil)
	c.Assert(fakeRuntime.imageList, check.HasLen, 0)
}

func (s *ComputeTestSuite) TestGarbageCollectImageNotOldEnough(c *check.C) {
	policy := &GCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
		MinAge:               time.Minute * 1,
	}
	manager, fakeRuntime, mockDiskCollector := newFakeImageGCManager(policy)
	mockDiskCollector.diskStats = &disk.UsageStat{
		Total:       1000,
		Used:        950,
		UsedPercent: 95.0,
	}
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 100},
	}
	var zero time.Time
	err := manager.detectImage(zero)
	c.Assert(err, check.IsNil)
	fakeRuntime.imageList = []types.ImageSummary{
		{ID: "test-a", Size: 100},
		{ID: "test-b", Size: 200},
	}
	fakeRuntime.containerList = []types.Container{
		{ID: "test-a-con", ImageID: "test-a"},
	}
	err = manager.detectImage(time.Now())
	c.Assert(err, check.IsNil)

	err = manager.GarbageCollect()
	c.Assert(err, check.NotNil)
	c.Assert(fakeRuntime.imageList, check.HasLen, 2)

	manager.policy.MinAge = time.Nanosecond * 1

	err = manager.GarbageCollect()
	c.Assert(err, check.IsNil)
	c.Assert(fakeRuntime.imageList, check.HasLen, 1)
	c.Assert(fakeRuntime.imageList[0].ID, check.Equals, "test-a")
}

func (s *ComputeTestSuite) TestIntegration(c *check.C) {
	// not a truly integration test as we still need to fake disk usage
	policy := &GCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
		MinAge:               time.Millisecond * 1,
	}
	manager, _, _ := newFakeImageGCManager(policy)
	dockerclient, err := client.NewEnvClient()
	if err != nil {
		c.Fatal(err)
	}
	//ignore error
	//_, err = dockerclient.ImageRemove(context.Background(), testImageUUID, types.ImageRemoveOptions{Force: true})
	//if err != nil {
	//	c.Fatal(err)
	//}
	manager.runtime = dockerclient
	manager.collector = &diskCollectorFake{}

	// start manager to detect images
	manager.Start()

	reader, err := dockerclient.ImagePull(context.Background(), testImageUUID, types.ImagePullOptions{})
	if err != nil {
		c.Fatal(err)
	}
	//wait for pulling
	_, err = ioutil.ReadAll(reader)
	if err != nil {
		c.Fatal(err)
	}
	inspect, _, err := dockerclient.ImageInspectWithRaw(context.Background(), testImageUUID)
	c.Assert(err, check.IsNil)
	manager.collector = &diskCollectorFake{
		diskStats: &disk.UsageStat{
			Used:        950,
			Total:       1000,
			UsedPercent: 95.0,
		},
	}
	err = manager.detectImage(time.Now())
	c.Assert(err, check.IsNil)
	var zero time.Time
	manager.imageRecords[inspect.ID].lastUsedTime = zero.Add(-time.Minute)
	//we don't care the error, but the image should be deleted
	manager.GarbageCollect()
	_, _, err = dockerclient.ImageInspectWithRaw(context.Background(), testImageUUID)
	c.Assert(err, check.NotNil)
}
