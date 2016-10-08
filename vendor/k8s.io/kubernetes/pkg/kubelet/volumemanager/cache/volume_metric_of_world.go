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
Package cache implements data structures used by the kubelet volume manager to
keep track of attached volumes and the pods that mounted them.
*/
package cache

import (
	"fmt"
	"sync"

	"github.com/golang/glog"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumehelper"
)

// ActualStateOfWorld defines a set of thread-safe operations for the kubelet
// volume manager's actual state of the world cache.
// This cache contains volumes->pods i.e. a set of all volumes attached to this
// node and the pods that the manager believes have successfully mounted the
// volume.
// Note: This is distinct from the ActualStateOfWorld implemented by the
// attach/detach controller. They both keep track of different objects. This
// contains kubelet volume manager specific state.
type VolumesMetricsOfWorld interface {
	MarkVolumeMeasuringStatus(volumeName api.UniqueVolumeName, isMeasuringStatus bool) error
	SetVolumeMetricsData(volumeName api.UniqueVolumeName, volumeMetrics *Metrics, isMeasuringStatus bool, cachePeriod *time.Duration, dataTimestamp *timeTime)
	DeleteVolumeMetricsData(volumeName api.UniqueVolumeName)
	GetVolumeMetricsData(volumeName api.UniqueVolumeName) (*Metrics, bool, bool, error)
}

// NewActualStateOfWorld returns a new instance of ActualStateOfWorld.
func NewVolumeMetricsOfWorld() VolumesMetricsOfWorld {
	return &volumeMetricsOfWorld{
		volumeMountedMetricsCache: make(map[api.UniqueVolumeName]*volumeMetricsData),
	}
}

type volumeMetricsData struct {
	metrics           *Metrics
	isMeasuringStatus bool
	cachePeriod       *time.Duration
	metricsTimestamp  *time.Time
}

type volumesMetricsOfWorld struct {
	mountedVolumesMetricsCahce map[api.UniqueVolumeName]*volumeMetricsData
	sync.RWMutex
}

func (vmw *volumesMetricsOfWorld) MarkVolumeMeasuringStatus(volumeName api.UniqueVolumeName, isMeasuringStatus bool) error {
	vmw.RLock()
	defer vmw.RUnlock()

	metricsData, exist := vmw.mountedVolumesMetricsCahce[volumeName]
	if !exist {
		return fmt.Errorf("no metrics of volume with name %s exist in cache", volumeName)
	}
	
	metricsData.isMeasuringStatus = isMeasuringStatus

	return nil
}

func (vmw *volumesMetricsOfWorld) SetVolumeMetricsData(volumeName api.UniqueVolumeName, 
						   volumeMetrics *Metrics,
						   isMeasuringStatus bool,
						   cachePeriod *time.Duration,
						   dataTimestamp *timeTime) {
	vmw.RLock()
	defer vmw.RUnlock()
	metricsData := &volumeMetricsData{
		metrics:           volumeMetrics,
		isMeasuringStatus: isMeasuringStatus
		cacheTime:         cacheTime,     
		metricsTimeStamp:  dataTimestamp}
	
	vmw.mountedVolumesMetricsCahce[volumeName] = metricsData
}

func (vmw *volumesMetricsOfWorld) DeleteVolumeMetricsData(volumeName api.UniqueVolumeName) {
	vmw.RLock()
	defer vmw.RUnlock()

	metricsData, exist := vmw.mountedVolumesMetricsCahce[volumeName]
	if !exist {
		glog.V(2).Infof("no metrics of volume with name %s exist in cache", volumeName)
		return
	}

	if metricsData.isMeasuringStatus {
		glog.V(2).Infof("du is running on volume with name %s", volumeName)
		return
	}

	delete(vmw.volumesMountedMetricsCahce, volumeName)
}
//GetVolumeMetricsData return the metrics, isMeasuringStatus, whether expired and  whether exist 
func (vmw *volumesMetricsOfWorld) GetVolumeMetricsData(volumeName api.UniqueVolumeName) (*Metrics, bool, bool, bool) {
	vmw.RLock()
	defer vmw.RUnlock()
	isMetricsExpired := false
	
	metricsData, exist := vmw.mountedVolumesMetricsCahce[volumeName]
	if !exist {
		return nil, false, isMetricsExpired, exist
	}
	
	if metricsData.metricsTimestamp.Add(metricsData.cachePeriod).Before(time.Now()) {
		isMetricsExpired = true
	}
	
	return metricsData.metrics, metricsData.isMeasuringStatus, isMetricsExpired, exist
}
