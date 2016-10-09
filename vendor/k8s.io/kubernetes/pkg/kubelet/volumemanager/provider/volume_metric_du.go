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
package provider

import (
	"errors"
	"fmt"

	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/volume/util"
)

const duTimeout = 1 * time.Minute

// VolumeMetricsProvider exposes volume metrics and volume name
// (e.g. used,available space) related to a volume.
type VolumeMetricsProvider interface {
	// GetMetrics calculates the volume usage and available space by executing "du".
	GetMetrics() (*Metrics, error)
	
	GetVolumeName() api.UniqueVolumeName
}

// volumeMetricsDu represents a VolumeMetricsProvider that calculates 
// the used and available Volume space by executing the "du" command.
type volumeMetricsDu struct {
	// the directory path the volume is mounted to.
	path       string
	volumeName api.UniqueVolumeName
	// capacity is from volume spec.
	capacity   *Quantity
}

// NewMetricsDu creates a new metricsDu with the Volume path.
func NewVolumeMetricsDu(path string, volumeName api.UniqueVolumeName, podName types.UniquePodName, capacity *Quantity) VolumeMetricsProvider {
	return &volumeMetricsDu{path:       path,
				volumeName: volumeName,
				capacity:   capacity}
}

func (vmd *volumeMetricsDu) GetMetrics() (*Metrics, error) {
	metrics := &Metrics{}
	if vmd.path == "" || vmd.capacity == nil {
		return nil, fmt.Errorf("no path defined for volume with name %s.", vmd.volumeName)
	}

	err := vmd.runDu(metrics)
	if err != nil {
		return nil, err
	}

	err := vmd.caculateVolumeMetrics(metrics)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func (vmd *volumeMetricsDu) GetVolumeName() api.UniqueVolumeName {
	return vmd.volumeName
}

// runDu executes the "du" command and writes the results to metrics.Used
func (vmd *volumeMetricsDu) runDu(metrics *Metrics) error {
	used, err := util.DuWithCheckingTimeout(vmd.path, duTimeout)
	if err != nil {
		return err
	}
	metrics.Used = used
	return nil
}

// caculateVolumeMetrics caculate volume availabel space.
func (vmd *volumeMetricsDu) caculateVolumeMetrics(metrics *Metrics) error {
	if metrics == nil || metrics.Used == nil {
		return fmt.Errorf("Fail to caculete metrics of volume with name %s due to unknow used of metrics.", vmd.volumeName)
	}
	metrics.Capacity = vmd.Capacity
	metrics.Availabel = resource.NewQuantity(uint64(vmd.capacity.Value()) - uint64(metrics.Used.Value()), resource.BinarySI)

	return nil
}
