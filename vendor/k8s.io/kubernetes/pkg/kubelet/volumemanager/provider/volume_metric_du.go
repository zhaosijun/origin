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
	"errors"
	"fmt"

	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/volume/util"
)

var _ MetricsProvider = &volumeMetricsDu{}

type volumeMetricsDu struct {
	path       string
	volumeName api.UniqueVolumeName
	capacity   *Quantity
}

// NewMetricsDu creates a new metricsDu with the Volume path.
func NewVolumeMetricsDu(path string, volumeName api.UniqueVolumeName, capacity *Quantity) MetricsProvider {
	return &volumeMetricsDu{path:       path,
				volumeName: volumeName,
				capacity:   capacity}
}

// GetMetrics calculates the volume usage and device free space by executing "du"
// and gathering filesystem info for the Volume path.
// See MetricsProvider.GetMetrics
func (vmd *volumeMetricsDu) GetMetrics() (*Metrics, error) {
	metrics := &Metrics{}
	if vmd.path == "" || vmd.capacity == nil {
		return metrics, errors.New("no path defined for volume with name %s.", vmd.volumeName)
	}

	err := vmd.runDu(metrics)
	if err != nil {
		return metrics, err
	}

	err := vmd.caculateVolumeMetrics(metrics)
	if err != nil {
		return metrics, err
	}

	return metrics, nil
}

// runDu executes the "du" command and writes the results to metrics.Used
func (vmd *volumeMetricsDu) runDu(metrics *Metrics) error {
	used, err := util.Du(vmd.path)
	if err != nil {
		return err
	}
	metrics.Used = used
	return nil
}

func (vmd *volumeMetricsDu) caculateVolumeMetrics(metrics *Metrics) error {
	if metrics == nil || metrics.Used == nil {
		return fmt.Errorf("Fail to caculete metrics of volume with name %s due to unknow used of metrics.", vmd.volumeName)
	}
	metrics.Capacity = vmd.Capacity
	metrics.Availabel = resource.NewQuantity(uint64(vmd.capacity.Value()) - uint64(metrics.Used.Value()), resource.BinarySI)

	return nil
}
