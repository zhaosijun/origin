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

// The maximum number of routine running `du` tasks at once.
const maxConsecutiveRoutinesRunningDu = 20

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


// DesiredStateOfWorldPopulator periodically loops through the list of active
// pods and ensures that each one exists in the desired state of the world cache
// if it has volumes. It also verifies that the pods in the desired state of the
// world cache still exist, if not, it removes them.
type VolumesMetricsOfWorldProvider interface {
	Run(stopCh <-chan struct{})
}

// NewDesiredStateOfWorldPopulator returns a new instance of
// DesiredStateOfWorldPopulator.
//
// kubeClient - used to fetch PV and PVC objects from the API server
// loopSleepDuration - the amount of time the populator loop sleeps between
//     successive executions
// podManager - the kubelet podManager that is the source of truth for the pods
//     that exist on this host
// desiredStateOfWorld - the cache to populate
func NewVolumesMetricsOfWorldProvider(
	loopSleepDuration time.Duration,
	getPodStatusRetryDuration time.Duration,
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

func (vmwp *volumesMetricsOfWorldProvider) startMeasureVolume(metricsProvider *VolumeMetricsProvider, cacheDuration time.Duration) {
	vmwp.volumesMetricsOfWorld.MarkVolumeMeasuringStatus(volumeName, true)
	claimRoutineTokenForRunningDu()
	defer releaseRoutineTokenForRunningDu()
	
	volumeName = metricsProvider.GetVolumeName()
	start := time.Now()
	//Call GetMetrics will block
	metrics := metricsProvider.GetMetrics()
	duration := time.Since(start)
	if duration > cacheDuration {
		glog.V(2).Infof("`du` on volume with name %s took %v", volume, duration)
		vmwp.volumesMetricsOfWorld.SetVolumeMetricsData(volumeName, metrics, false, duration*2, time.Now())
	} else {
		vmwp.volumesMetricsOfWorld.SetVolumeMetricsData(volumeName, metrics, false, cacheDuration , time.Now())
	}
	
}

func (vmwp *volumesMetricsOfWorldProvider) Run(stopCh <-chan struct{}) {
	for {
		select {
		case <- stopCh:
			return
		case <- time.After(vmwp.loopSleepDuration):
			volumeMetricsProviders := vmwp.actualStateOfWorld.GetMountedVolumeMetricsProviders()
			for path, _ := range volumeMetricsProviders {
				metricsProvider = volumeMetricsProviders[path]
				volumeMetrics, measureRequired, exist := vmwp.volumesMetricsOfWorld.GetVolumeMetricsData(metricsProvider.GetVolumeName())
				if !exist || measureRequired {
					go vmwp.startMeasureVolume(metricsProvider, metricsCacheDuration)
				}
			}
		}
	}
}
