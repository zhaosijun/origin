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

// VolumesMetricsOfWorld defines a set of thread-safe operations for the kubelet
// volume manager's volumes metrics of the world cache.
// This cache contains volumes->metrics data.
type VolumesMetricsOfWorld interface {
	// MarkVolumeMeasuringStatus marks the volume as being measured or not. 
	// The volume may be mounted on multiple paths for multiple pods. 
	// It is neccery to avoid excute 'du' on all paths that mountes the same volume. 
	// When mark it as true indicates the volume is being measuring.
	// if the volume does not exist in the cache , an error is returned.
	MarkVolumeMeasuringStatus(volumeName api.UniqueVolumeName, measuringIsDoing bool) error
	
	// SetVolumeMetricsData sets the volume metrics data value for the given volume. 
	// volumeMetrics is the metrics of the volume.
	// measuringIsDoing indicates whether the volume is being measured. 
	// cacheDuration is the amount of time the metrics is effective in the cache.
	// dataTimestamp is the time of last measure of the metrics.
	SetVolumeMetricsData(volumeName api.UniqueVolumeName, volumeMetrics *Metrics, measuringIsDoing bool, cacheDuration *time.Duration, dataTimestamp *timeTime)
	
	// DeleteVolumeMetricsData removes the given volume metrics from the 
	// cache when the volume has not been in the actual world.
	DeleteVolumeMetricsData(volumeName api.UniqueVolumeName)
	
	// GetVolumeMetricsData return the metrics, whether volume needs to be measured. 
	// If the metrics data is expired and the volume is not being measured,
	// return the seconde result with true indicates volume needs to be measured.
	GetVolumeMetricsData(volumeName api.UniqueVolumeName) (*Metrics, bool)
}

// NewVolumeMetricsOfWorld returns a new instance of VolumesMetricsOfWorld.
func NewVolumeMetricsOfWorld() VolumesMetricsOfWorld {
	return &volumeMetricsOfWorld{
		volumeMountedMetricsCache: make(map[api.UniqueVolumeName]*volumeMetricsData),
	}
}

type volumeMetricsData struct {
	metrics           *Metrics
	measuringIsDoing  bool
	cacheDuration     *time.Duration
	metricsTimestamp  *time.Time
}

type volumesMetricsOfWorld struct {
	mountedVolumesMetricsCahce map[api.UniqueVolumeName]*volumeMetricsData
	sync.RWMutex
}

func (vmw *volumesMetricsOfWorld) MarkVolumeMeasuringStatus(volumeName api.UniqueVolumeName, measuringIsDoing bool) error {
	vmw.RLock()
	defer vmw.RUnlock()

	metricsData, exist := vmw.mountedVolumesMetricsCahce[volumeName]
	if !exist {
		return fmt.Errorf("no metrics of volume with name %s exist in cache", volumeName)
	}
	
	metricsData.measuringIsDoing = measuringIsDoing

	return nil
}

func (vmw *volumesMetricsOfWorld) SetVolumeMetricsData(volumeName api.UniqueVolumeName, 
						   volumeMetrics *Metrics,
						   measuringIsDoing bool,
						   cacheDuration *time.Duration,
						   dataTimestamp *timeTime) {
	vmw.RLock()
	defer vmw.RUnlock()
	metricsData := &volumeMetricsData{
		metrics:           volumeMetrics,
		measuringIsDoing:  measuringIsDoing
		cacheDuration:     cacheDuration,     
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

	delete(vmw.volumesMountedMetricsCahce, volumeName)
}

func (vmw *volumesMetricsOfWorld) GetVolumeMetricsData(volumeName api.UniqueVolumeName) (*Metrics, bool, bool) {
	vmw.RLock()
	defer vmw.RUnlock()
	measureRequired := true
	
	metricsData, exist := vmw.mountedVolumesMetricsCahce[volumeName]
	if !exist {
		return nil, measureRequired
	}
	
	metricsIsExpired := metricsData.metricsTimestamp.Add(metricsData.cacheDuration).Before(time.Now())
	if metricsData.measuringIsDoing || !metricsIsExpired {
		measureRequired = false
	}
	
	return metricsData.metrics, measureRequired
}
