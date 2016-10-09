// +build linux darwin

/*
Copyright 2014 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"k8s.io/kubernetes/pkg/api/resource"
)

// FSInfo linux returns (available bytes, byte capacity, byte usage, error) for the filesystem that
// path resides upon.
func FsInfo(path string) (int64, int64, int64, error) {
	statfs := &syscall.Statfs_t{}
	err := syscall.Statfs(path, statfs)
	if err != nil {
		return 0, 0, 0, err
	}
	// TODO(vishh): Include inodes space
	// Available is blocks available * fragment size
	available := int64(statfs.Bavail) * int64(statfs.Bsize)

	// Capacity is total block count * fragment size
	capacity := int64(statfs.Blocks) * int64(statfs.Bsize)

	// Usage is block being used * fragment size (aka block size).
	usage := (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize)

	return available, capacity, usage, nil
}

func Du(path string) (*resource.Quantity, error) {
	// Uses the same niceness level as cadvisor.fs does when running du
	// Uses -B 1 to always scale to a blocksize of 1 byte
	out, err := exec.Command("nice", "-n", "19", "du", "-s", "-B", "1", path).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed command 'du' ($ nice -n 19 du -s -B 1) on path %s with error %v", path, err)
	}
	used, err := resource.ParseQuantity(strings.Fields(string(out))[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'du' output %s due to error %v", out, err)
	}
	used.Format = resource.BinarySI
	return &used, nil
}

func DuWithCheckingTimeout(path string, timeout *time.Duration) (*resource.Quantity, error) {
	// Uses the same niceness level as cadvisor.fs does when running du
	// Uses -B 1 to always scale to a blocksize of 1 byte
	cmd := exec.Command("nice", "-n", "19", "du", "-s", "-B", "1", path)
	stdoutp, err := cmd.StdoutPipe()
	if err != nil {
		return 0, fmt.Errorf("failed to setup stdout for cmd %v - %v", cmd.Args, err)
	}
	stderrp, err := cmd.StderrPipe()
	if err != nil {
		return 0, fmt.Errorf("failed to setup stderr for cmd %v - %v", cmd.Args, err)
	}

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("failed to exec du - %v", err)
	}
	stdoutb, souterr := ioutil.ReadAll(stdoutp)
	stderrb, _ := ioutil.ReadAll(stderrp)
	timer := time.AfterFunc(timeout, func() {
		glog.Infof("killing cmd %v due to timeout(%s)", cmd.Args, timeout.String())
		cmd.Process.Kill()
	})
	err = cmd.Wait()
	timer.Stop()
	if err != nil {
		return 0, fmt.Errorf("du command failed on %s with output stdout: %s, stderr: %s - %v", dir, string(stdoutb), string(stderrb), err)
	}
	stdout := string(stdoutb)
	if souterr != nil {
		glog.Errorf("failed to read from stdout for cmd %v - %v", cmd.Args, souterr)
	}
	used, err := resource.ParseQuantity(strings.Fields(stdout)[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'du' output %s due to error %v", stdout, err)
	}
	used.Format = resource.BinarySI
	return &used, nil
}
