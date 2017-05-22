package main

import (
	"os"

	"strconv"
	"time"

	"context"
	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/client"
	"github.com/rancher/gc-manager/imagegc"
	"github.com/urfave/cli"
)

var VERSION = "v0.0.0-dev"

const (
	HighThreshold = "IMAGE_GC_HIGH_THRESHOLD"
	LowThreshold  = "IMAGE_GC_LOW_THRESHOLD"
	MinAge        = "IMAGE_GC_MIN_AGE"
	GCInterval    = "IMAGE_GC_INTERVAL"
)

func main() {
	app := cli.NewApp()
	app.Name = "gc-manager"
	app.Version = VERSION
	app.Usage = "You need help!"
	app.Action = run
	app.Run(os.Args)
}

func run(c *cli.Context) error {
	if os.Getenv("RANCHER_DEBUG") == "true" {
		logrus.SetLevel(logrus.DebugLevel)
	}

	dockerclient, err := client.NewEnvClient()
	if err != nil {
		return err
	}
	highThresholdPercent, err := strconv.ParseFloat(os.Getenv(HighThreshold), 64)
	if err != nil {
		highThresholdPercent = 90.0
	}
	lowThresholdPercent, err := strconv.ParseFloat(os.Getenv(LowThreshold), 64)
	if err != nil {
		lowThresholdPercent = 80.0
	}
	minAge, err := strconv.Atoi(os.Getenv(MinAge))
	if err != nil {
		minAge = 5
	}
	interval, err := strconv.Atoi(os.Getenv(GCInterval))
	if err != nil {
		interval = 5
	}
	info, err := dockerclient.Info(context.Background())
	if err != nil {
		return err
	}

	policy := &imagegc.GCPolicy{
		HighThresholdPercent: highThresholdPercent,
		LowThresholdPercent:  lowThresholdPercent,
		MinAge:               time.Minute * time.Duration(minAge),
		GCInterval:           time.Minute * time.Duration(interval),
	}
	gcManager, err := imagegc.NewImageManagerImpl(policy, dockerclient, info.Driver)
	if err != nil {
		return err
	}
	gcManager.Start()
	for {
		err := gcManager.GarbageCollect()
		if err != nil {
			logrus.Errorf("Image garbage collection failed: %v", err)
		}
		time.Sleep(policy.GCInterval)
	}
}
