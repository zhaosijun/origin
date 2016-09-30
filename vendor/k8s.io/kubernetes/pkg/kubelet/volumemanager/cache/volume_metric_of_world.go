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
	GetVolumeMetrics(volumeName api.UniqueVolumeName) (*Metrics, error)
}

type MeasureStage int

const (
	DuRuning = iota
	DuComplete
)

// NewActualStateOfWorld returns a new instance of ActualStateOfWorld.
func NewVolumeMetricsOfWorld() VolumesMetricsOfWorld {
	return &volumeMetricsOfWorld{
		volumeMountedMetricsCache: make(map[api.UniqueVolumeName]*Metrics),
	}
}

type VolumeMetricsData struct {
	metrics          *Metrics
	measureStage     MeasureStage
	metricsTimestamp *time.Time
}

type volumesMetricsOfWorld struct {
	volumesMountedMetricsCahce map[api.UniqueVolumeName]*VolumeMetricsData
	sync.RWMutex
}

func NewVolumeMetricsData(metrics *Metrics,
	measureStage MeasureStage,
	metricsTimestamp *time.Time) {
	return &VolumeMetricsData{
		metrics:          metrics,
		measureStage:     measureStage,
		metricsTimeStamp: metricsTimestamp}
}

func (vmw *volumesMetricsOfWorld) SetVolumeMeasureStage(volumeName api.UniqueVolumeName, measureStage MeasureStage) {
	vmw.RLock()
	defer vmw.RUnlock()

	volumeMetrics, exist := vmw.volumesMountedMetricsCahce[volumeName]
	if !exist {
		return nil, fmt.Errorf("no metrics of volume with name %s exist in cache", volumeName)
	}
	volumeMetrics.measureStage = measureStage
}

func (vmw *volumesMetricsOfWorld) SetVolumeMetrics(volumeName api.UniqueVolumeName, volumeMetrics *VolumeMetricsData) {
	vmw.RLock()
	defer vmw.RUnlock()

	vmw.volumesMountedMetricsCahce[volumeName] = volumeMetrics
}

func (vmw *volumesMetricsOfWorld) DeleteVolumeMetrics(volumeName api.UniqueVolumeName) {
	vmw.RLock()
	defer vmw.RUnlock()

	volumeMetrics, exist := vmw.volumesMountedMetricsCahce[volumeName]
	if !exist {
		glog.V(2).Infof("no metrics of volume with name %s exist in cache", volumeName)
		return
	}

	if volumeMetrics.stageMeasure == DuRunning {
		glog.V(2).Infof("du is running on volume with name %s", volumeName)
		return
	}

	delete(vmw.volumesMountedMetricsCahce, volumeName)
}

func (vmw *volumesMetricsOfWorld) GetVolumeMetrics(volumeName api.UniqueVolumeName) (*Metrics, error) {
	vmw.RLock()
	defer vmw.RUnlock()

	volumeMetrics, exist := vmw.volumesMountedMetricsCahce[volumeName]
	if !exist {
		return nil, fmt.Errorf("no metrics of volume with name %s exist in cache", volumeName)
	}
	return volumeMetrics.metrics, nil
}
