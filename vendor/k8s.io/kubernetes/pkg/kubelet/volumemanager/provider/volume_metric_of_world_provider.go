/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package populator implements interfaces that monitor and keep the states of the
caches in sync with the "ground truth".
*/
package provider

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/volume"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumehelper"
)

const (
	longDu                          = 1 * time.Second
	maxDuBackoffFactor              = 20
	// The maximum number of routine that is running `du` tasks at once.
	maxConsecutiveRoutinesRunningDu = 20
)

// A pool for restricting the number of consecutive routines running `du` tasks.
var routinePoolForRunningDu = make(chan struct{}, maxConsecutiveRoutinesRunningDu)

func init() {
	for i := 0; i < maxConsecutiveRoutinesRunningDu; i++ {
		releaseRoutineTokenForRunningDu()
	}
}

func claimRoutineTokenForRunningDu() {
	<-routinePoolForRunningDu
}

func releaseRoutineTokenForRunningDu() {
	routinePoolForRunningDu <- struct{}{}
}


// VolumesMetricsOfWorldProvider periodically loops through the map containing  
// metrics providers which generated from actual world cache and update each 
// volume by excuting 'du' on the path to which the volume should be mounted for
// a pod.
type VolumesMetricsOfWorldProvider interface {
	// Starts running the provider loop which executes periodically, checks
	// if volume metrics that should be updated will be measured by excuting 'du'.
	Run(stopCh <-chan struct{})
}

// NewVolumesMetricsOfWorldProvider returns a new instance of
// NewVolumesMetricsOfWorldProvider.
//
// metricsCacheDuration - the amount of time the metrics data is effective
// 	in the cache
// loopSleepDuration - the amount of time the provider loop sleeps between
//     successive executions
// actualStateOfWorld - the cache of actual world
// volumesMetricsOfWorld - the cache to keep volume metrics data
func NewVolumesMetricsOfWorldProvider(
	metricsCacheDuration time.Duration,
	loopSleepDuration time.Duration,
	actualStateOfWorld cache.ActualStateOfWorld,
	volumesMetricsOfWorld cache.VolumesMetricsOfWorld) VolumesMetricsOfWorldProvider {
	return &volumesMetricsOfWorldProvider{
		metricsCacheDuration:      metricsCacheDuration,
		loopSleepDuration:         loopSleepDuration,
		volumesMetricsOfWorld:     actualStateOfWorld,
		actualStateOfWorld:        actualStateOfWorld,
	}
}

type volumesMetricsOfWorldProvider struct {
	metricsCacheDuration      time.Duration
	loopSleepDuration         time.Duration
	actualStateOfWorld        cache.DesiredStateOfWorld
	volumesMetricsOfWorld     cache.VolumesMetricsOfWorld
}

// startMeasureVolume start VolumeMetricsProvider to update volume metrics.
// cacheDuration is from the volume metrics data in cache. 
func (vmwp *volumesMetricsOfWorldProvider) startMeasureVolume(metricsProvider *VolumeMetricsProvider, cacheDuration time.Duration) {
	volumeName = metricsProvider.GetVolumeName()
	// It should mark the volume is being measured before take a Token.
	// Otherwise paths to which the same volume is mounted may take token.
	vmwp.volumesMetricsOfWorld.MarkVolumeMeasuringStatus(volumeName, true)
	// startMeasureVolume Call will block here until the number of 'du' task 
	// running at once is less than maxConsecutiveRoutinesRunningDu. 
	claimRoutineTokenForRunningDu()
	defer releaseRoutineTokenForRunningDu()
	
	start := time.Now()
	// GetMetrics Call maybe block here for some time.
	metrics, err := metricsProvider.GetMetrics()
	// If fail to get the volume metrics, double the cache time.
	// The cache time is not greater than maxDuBackoffFactor*vmwp.metricsCacheDuration.
	// Just as cadvisor does.
	if err != nil {
		glog.Errorf("failed to excute 'du' on volume with name %s - %v", volumeName, err)
		cacheDuration = cacheDuration*2
		if cacheDuration > maxDuBackoffFactor*vmwp.metricsCacheDuration {
			cacheDuration = maxDuBackoffFactor*vmwp.metricsCacheDuration
		}
		vmwp.volumesMetricsOfWorld.SetVolumeMetricsData(volumeName, metrics, false, cacheDuration, time.Now())
		return
	}
	
	duration := time.Since(start)
	if duration > longDu {
		glog.V(2).Infof("`du` on volume with name %s took %v", volumeName, duration)
	}
	
	vmwp.volumesMetricsOfWorld.SetVolumeMetricsData(volumeName, metrics, false, vmwp.metricsCacheDuration, time.Now())
}

func (vmwp *volumesMetricsOfWorldProvider) Run(stopCh <-chan struct{}) {
	init()
	for {
		select {
		case <- stopCh:
			return
		case <- time.After(vmwp.loopSleepDuration):
			volumeMetricsProviders := vmwp.actualStateOfWorld.GetMountedVolumeMetricsProviders()
			for path, _ := range volumeMetricsProviders {
				metricsProvider = volumeMetricsProviders[path]
				//A volume mabye mounted to multiple paths. Excute 'du' on one.    
				volumeMetrics, measureRequired, cacheDuration := vmwp.volumesMetricsOfWorld.GetVolumeMetricsData(metricsProvider.GetVolumeName())
				//Set cache duration of new volume metrics as value 'vmwp.metricsCacheDuration'.
				if volumeMetrics == nil {
					cacheDuration = vmwp.metricsCacheDuration
				}
				if measureRequired {
					go vmwp.startMeasureVolume(metricsProvider, cacheDuration)
				}
			}
		}
	}
}
