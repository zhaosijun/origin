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

// The maximum number of `du` tasks that can be running at once.
const maxConsecutiveDus = 20

// A pool for restricting the number of consecutive `du` tasks running.
var duPool = make(chan struct{}, maxConsecutiveDus)

func init() {
	for i := 0; i < maxConsecutiveDus; i++ {
		releaseDuToken()
	}
}

func claimDuToken() {
	<-duPool
}

func releaseDuToken() {
	duPool <- struct{}{}
}


// DesiredStateOfWorldPopulator periodically loops through the list of active
// pods and ensures that each one exists in the desired state of the world cache
// if it has volumes. It also verifies that the pods in the desired state of the
// world cache still exist, if not, it removes them.
type VolumesMetricsOfWorldProvider interface {
	Run(stopCh <-chan struct{})

	ReprocessPod(podName volumetypes.UniquePodName)
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
	kubeClient internalclientset.Interface,
	loopSleepDuration time.Duration,
	getPodStatusRetryDuration time.Duration,
	podManager pod.Manager,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	kubeContainerRuntime kubecontainer.Runtime) DesiredStateOfWorldPopulator {
	return &desiredStateOfWorldPopulator{
		kubeClient:                kubeClient,
		loopSleepDuration:         loopSleepDuration,
		getPodStatusRetryDuration: getPodStatusRetryDuration,
		podManager:                podManager,
		desiredStateOfWorld:       desiredStateOfWorld,
		pods: processedPods{
			processedPods: make(map[volumetypes.UniquePodName]bool)},
		kubeContainerRuntime: kubeContainerRuntime,
	}
}

type volumesMetricsOfWorldProvider struct {
	kubeClient                internalclientset.Interface
	loopSleepDuration         time.Duration
	getPodStatusRetryDuration time.Duration
	actualStateOfWorld        cache.DesiredStateOfWorld
	volumesMetricsOfWorld     cache.VolumesMetricsOfWorld
	volumes                   volumeToMountedPoints
	timeOfLastGetPodStatus    time.Time
}

type volumeToMountedPoints struct {
	mountedPoints map[api.UniqueVolumeName][]string
	capacity      *Quantity
	sync.RWMutex
}

func (vmwp *volumesMetricsOfWorldProvider) runDu(path string) (*Metrics, error) {
	metrics := &Metrics{}
	metricProvider = NewMetricsDu(path)
	claimDuToken()
	defer releaseDuToken()
	vmwp.volumesMetricsOfWorld.SetVolumeMeasureStage(DuRunning)
	err := metricProvider.runDu(metrics)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

func caculateVolumeMetrics(volumeMountedPoint *VolumeMountedPoint, metrics *Metrics) error {
	if volumeMountedPoint == nil || metrics == nil{
		return fmt.Errorf("volume mounted point or metrics is nil")
	}

	availabel := uint64(volumeMountedPoint.Capacity.Value()) - uint64(metrics.Used.Value())
	metrics.Capacity = volumeMountedPoint.Capacity
	metrics.Availabel = resource.NewQuantity(availabel, resource.BinarySI)

	return metrics
}


func (vmwp *volumesMetricsOfWorldProvider) Run(stopCh <-chan struct{}) {
	for {
		select {
		case <- stopCh:
			return
		case <- time.After(vmwp.loopSleepDuration):
			start := time.Now()
			volumeMountedPoints := vmwp.actualStateOfWorld.GetVolumeMountedPoints()
			for mountedPath, volumeMountedPoint := range volumeMountedPoints {
				volumeMetrics, err := vmwp.volumesMetricsOfWorld.GetVolumeMetricsNewThan(volumeMountedPoint.VolumeName)
				if err == nil {
					glog.V(2).Infof("%v: %v")
					continue
				}
				volumeMetrics, err = wmwp.volumesMetricsOfWorld.runDu(mountedPath)
				if err != nil {
					vmwp.volumesMetricsOfWorld.SetVolumeMeasureStage(DuComplete)
					continue
				}
				err := caculateVolumeMetrics(volumeMountedPoint, volumeMetrics)
				if err != nil {
					vmwp.volumesMetricsOfWorld.SetVolumeMeasureStage(DuComplete)
					continue
				}
				vmwp.volumesMetricsOfWorld.SetVolumeMetrics(NewVolumeMetricsData{
					metrics: volumeMetrics,
					measureStage: DuComplete,
					metricsTimestamp: time.Now()})
			}
			duration := time.Since(start)
			if duration > longDu {
				glog.V(2).Infof("`du` on following dirs took %v: %v", duration, []string{fh.rootfs, fh.extraDir})
			}
		}
	}
	//wait.Until(vmwp.providerLoopFunc(), vmwp.loopSleepDuration, stopCh)
}

func (vmwp *volumesMetricsOfWorldProvider) providerLoopFunc() func() {
	return func() {
		volumeMountedPoints := vmwp.actualStateOfWorld.GetVolumeMountedPoints()
	}
}

// Iterate through all pods and add to desired state of world if they don't
// exist but should
func (vmwp *volumesMetricsOfWorldProvider) findAndAddVolumeMountedPoints() {
	volumeMountedPoints := vmwp.actualStateOfWorld.GetVolumeMountedPoints()
	for _, volumeMountedPoint := range volumeMountedPoints {
		vmwp.addVolumeMountedPoint(volumeMountedPoint)
	}
}

func (vmwp *volumesMetricsOfWorldProvider) findAndRemoveMountedVolumePoints() {
	volumeSpecs := vmwp.actualStateOfWorld.GetMountedVolumeSpecs()
	for volumeName, _ := range vmwp.volumes.mountedPoints {
		vmwp.removeMountedVolumePoint(volumeSpecs, volumeName)
	}
}

func (vmwp *volumesMetricsOfWorldProvider) removeMountedVolumePoint(volumeSpecs []MountedVolumeSpec, volumeName api.UniqueVolumeName) {
	_, exist = volumeSpecs[]
}

func (vmwp *volumesMetrscsOfWorldProvider) addVolumeMountedPoint(volumeMountedPoint VolumeMountedPoint) {
	var mountedPoints []string
	if podVolumeKey == nil || volumeSpec == nil {
		return
	}

	vmwp.volumes.capacity = volumeSpec.Spec.PersistentVolume.PersistentVolumeSpec.Capacity[ResourceStorage]
	mountedPoint = volumeSpec.Mounter.GetPath()

	mountedPointsExisted, exist := vmwp.volumes.mountedPoints[podVolumeKey.VolumeName]
	if !exist {
		vmwp.volumes.mountedPoints[podVolumeKey.VolumeName] = append(mountedPoints, mountedPoint)
		return
	}

	vmwp.volumes.mountedPoints[podvolumeKey.VolumeName] = append(mountedPointsExisted, mountedPoint)
}

// podPreviouslyProcessed returns true if the volumes for this pod have already
// been processed by the populator
func (dswp *desiredStateOfWorldPopulator) podPreviouslyProcessed(
	podName volumetypes.UniquePodName) bool {
	dswp.pods.RLock()
	defer dswp.pods.RUnlock()

	_, exists := dswp.pods.processedPods[podName]
	return exists
}

// markPodProcessed records that the volumes for the specified pod have been
// processed by the populator
func (dswp *desiredStateOfWorldPopulator) markPodProcessed(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	dswp.pods.processedPods[podName] = true
}

// markPodProcessed removes the specified pod from processedPods
func (dswp *desiredStateOfWorldPopulator) deleteProcessedPod(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	delete(dswp.pods.processedPods, podName)
}

// createVolumeSpec creates and returns a mutatable volume.Spec object for the
// specified volume. It dereference any PVC to get PV objects, if needed.
func (dswp *desiredStateOfWorldPopulator) createVolumeSpec(
	podVolume api.Volume, podNamespace string) (*volume.Spec, string, error) {
	if pvcSource :=
		podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
		glog.V(10).Infof(
			"Found PVC, ClaimName: %q/%q",
			podNamespace,
			pvcSource.ClaimName)

		// If podVolume is a PVC, fetch the real PV behind the claim
		pvName, pvcUID, err := dswp.getPVCExtractPV(
			podNamespace, pvcSource.ClaimName)
		if err != nil {
			return nil, "", fmt.Errorf(
				"error processing PVC %q/%q: %v",
				podNamespace,
				pvcSource.ClaimName,
				err)
		}

		glog.V(10).Infof(
			"Found bound PV for PVC (ClaimName %q/%q pvcUID %v): pvName=%q",
			podNamespace,
			pvcSource.ClaimName,
			pvcUID,
			pvName)

		// Fetch actual PV object
		volumeSpec, volumeGidValue, err :=
			dswp.getPVSpec(pvName, pvcSource.ReadOnly, pvcUID)
		if err != nil {
			return nil, "", fmt.Errorf(
				"error processing PVC %q/%q: %v",
				podNamespace,
				pvcSource.ClaimName,
				err)
		}

		glog.V(10).Infof(
			"Extracted volumeSpec (%v) from bound PV (pvName %q) and PVC (ClaimName %q/%q pvcUID %v)",
			volumeSpec.Name,
			pvName,
			podNamespace,
			pvcSource.ClaimName,
			pvcUID)

		return volumeSpec, volumeGidValue, nil
	}

	// Do not return the original volume object, since the source could mutate it
	clonedPodVolumeObj, err := api.Scheme.DeepCopy(podVolume)
	if err != nil || clonedPodVolumeObj == nil {
		return nil, "", fmt.Errorf(
			"failed to deep copy %q volume object. err=%v", podVolume.Name, err)
	}

	clonedPodVolume, ok := clonedPodVolumeObj.(api.Volume)
	if !ok {
		return nil, "", fmt.Errorf(
			"failed to cast clonedPodVolume %#v to api.Volume",
			clonedPodVolumeObj)
	}

	return volume.NewSpecFromVolume(&clonedPodVolume), "", nil
}

// getPVCExtractPV fetches the PVC object with the given namespace and name from
// the API server extracts the name of the PV it is pointing to and returns it.
// An error is returned if the PVC object's phase is not "Bound".
func (dswp *desiredStateOfWorldPopulator) getPVCExtractPV(
	namespace string, claimName string) (string, types.UID, error) {
	pvc, err :=
		dswp.kubeClient.Core().PersistentVolumeClaims(namespace).Get(claimName)
	if err != nil || pvc == nil {
		return "", "", fmt.Errorf(
			"failed to fetch PVC %s/%s from API server. err=%v",
			namespace,
			claimName,
			err)
	}

	if pvc.Status.Phase != api.ClaimBound || pvc.Spec.VolumeName == "" {
		return "", "", fmt.Errorf(
			"PVC %s/%s has non-bound phase (%q) or empty pvc.Spec.VolumeName (%q)",
			namespace,
			claimName,
			pvc.Status.Phase,
			pvc.Spec.VolumeName)
	}

	return pvc.Spec.VolumeName, pvc.UID, nil
}

// getPVSpec fetches the PV object with the given name from the API server
// and returns a volume.Spec representing it.
// An error is returned if the call to fetch the PV object fails.
func (dswp *desiredStateOfWorldPopulator) getPVSpec(
	name string,
	pvcReadOnly bool,
	expectedClaimUID types.UID) (*volume.Spec, string, error) {
	pv, err := dswp.kubeClient.Core().PersistentVolumes().Get(name)
	if err != nil || pv == nil {
		return nil, "", fmt.Errorf(
			"failed to fetch PV %q from API server. err=%v", name, err)
	}

	if pv.Spec.ClaimRef == nil {
		return nil, "", fmt.Errorf(
			"found PV object %q but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim",
			name)
	}

	if pv.Spec.ClaimRef.UID != expectedClaimUID {
		return nil, "", fmt.Errorf(
			"found PV object %q but its pv.Spec.ClaimRef.UID (%q) does not point to claim.UID (%q)",
			name,
			pv.Spec.ClaimRef.UID,
			expectedClaimUID)
	}

	volumeGidValue := getPVVolumeGidAnnotationValue(pv)
	return volume.NewSpecFromPersistentVolume(pv, pvcReadOnly), volumeGidValue, nil
}

func getPVVolumeGidAnnotationValue(pv *api.PersistentVolume) string {
	if volumeGid, ok := pv.Annotations[volumehelper.VolumeGidAnnotationKey]; ok {
		return volumeGid
	}

	return ""
}

