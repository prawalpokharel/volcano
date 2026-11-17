/*
Copyright 2026 The Volcano Authors.

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

package cache

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/core/v1"

	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"
	k8sutil "volcano.sh/volcano/pkg/scheduler/plugins/util/k8s"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	// benchPodsPerNode keeps ten running Pods on every node, the density of
	// the 10,000 Pods on 1,000 nodes scenario in issue #5941.
	benchPodsPerNode = 10
	// benchNodeRemoveInterval is how often, in scheduling cycles, the high
	// churn case removes one node and brings the previously removed one back.
	benchNodeRemoveInterval = 10
)

// snapshotBenchCluster is a mock cache filled with synthetic nodes and Pods,
// plus the bookkeeping needed to keep changing it in a deterministic order.
type snapshotBenchCluster struct {
	sc        *SchedulerCache
	nodeNames []string
	// pods holds the tasks currently placed on each node, by node index.
	pods [][]*schedulingapi.TaskInfo
	// removed is the index of the node that is currently out of the cache,
	// or -1 when every node is present.
	removed int
	podSeq  int
}

func newSnapshotBenchCluster(tb testing.TB, nodes int) *snapshotBenchCluster {
	tb.Helper()
	sc, schedulingQueue := mockForTest()
	tb.Cleanup(sc.cancel)
	// RemoveNode clears the bind record of the node, so the cache needs its binder.
	sc.ConflictAwareBinder = NewConflictAwareBinder(sc, schedulingQueue)

	c := &snapshotBenchCluster{
		sc:        sc,
		nodeNames: make([]string, nodes),
		pods:      make([][]*schedulingapi.TaskInfo, nodes),
		removed:   -1,
	}
	for i := range nodes {
		c.nodeNames[i] = fmt.Sprintf("node-%04d", i)
		c.addNode(tb, i, "64")
		for range benchPodsPerNode {
			c.addPod(tb, i)
		}
	}
	return c
}

func (c *snapshotBenchCluster) addNode(tb testing.TB, idx int, cpu string) {
	node := util.BuildNode(c.nodeNames[idx], schedulingapi.BuildResourceList(cpu, "256Gi",
		schedulingapi.ScalarResource{Name: "pods", Value: "110"}), nil)
	if err := c.sc.AddOrUpdateNode(node); err != nil {
		tb.Fatalf("AddOrUpdateNode(%s) failed: %v", c.nodeNames[idx], err)
	}
}

func (c *snapshotBenchCluster) addPod(tb testing.TB, idx int) {
	c.podSeq++
	pod := util.BuildPod("bench", fmt.Sprintf("pod-%d", c.podSeq), c.nodeNames[idx],
		v1.PodRunning, schedulingapi.BuildResourceList("100m", "128Mi"), "", nil, nil)
	task := schedulingapi.NewTaskInfo(pod)

	c.sc.Mutex.Lock()
	err := c.sc.addTask(task)
	c.sc.Mutex.Unlock()
	if err != nil {
		tb.Fatalf("addTask on %s failed: %v", c.nodeNames[idx], err)
	}
	c.pods[idx] = append(c.pods[idx], task)
}

func (c *snapshotBenchCluster) deleteOldestPod(idx int) {
	if len(c.pods[idx]) == 0 {
		return
	}
	task := c.pods[idx][0]
	c.pods[idx] = c.pods[idx][1:]

	c.sc.Mutex.Lock()
	c.sc.deleteTask(task)
	c.sc.Mutex.Unlock()
}

// bind replaces one Pod on a node, which is the cache change every scheduled
// Pod causes: the node's generation moves forward and the node goes to the
// head of the cache's node list.
func (c *snapshotBenchCluster) bind(tb testing.TB, cycle int) {
	idx := cycle % len(c.nodeNames)
	if idx == c.removed {
		idx = (idx + 1) % len(c.nodeNames)
	}
	c.deleteOldestPod(idx)
	c.addPod(tb, idx)
}

// churnNodes changes node objects the way a busy cluster does: one node gets
// new allocatable resources every cycle, and every benchNodeRemoveInterval
// cycles one node leaves the cluster while the node removed earlier returns.
func (c *snapshotBenchCluster) churnNodes(tb testing.TB, cycle int) {
	n := len(c.nodeNames)

	if idx := (cycle*7 + 3) % n; idx != c.removed {
		cpu := "64"
		if cycle%2 == 0 {
			cpu = "96"
		}
		c.addNode(tb, idx, cpu)
	}

	if cycle%benchNodeRemoveInterval != 0 {
		return
	}
	if c.removed >= 0 {
		c.addNode(tb, c.removed, "64")
		for range benchPodsPerNode {
			c.addPod(tb, c.removed)
		}
	}
	idx := (cycle / benchNodeRemoveInterval * 13) % n
	for len(c.pods[idx]) > 0 {
		c.deleteOldestPod(idx)
	}
	if err := c.sc.RemoveNode(c.nodeNames[idx]); err != nil {
		tb.Fatalf("RemoveNode(%s) failed: %v", c.nodeNames[idx], err)
	}
	c.removed = idx
}

// verify fails unless the snapshot lists exactly the ready nodes of the cache
// and the same number of Pods on each of them.
func (c *snapshotBenchCluster) verify(tb testing.TB, snapshot *k8sutil.Snapshot) {
	tb.Helper()
	if err := c.sc.UpdateSnapshot(snapshot); err != nil {
		tb.Fatalf("UpdateSnapshot failed: %v", err)
	}

	c.sc.Mutex.RLock()
	defer c.sc.Mutex.RUnlock()

	got := snapshot.GetVolcanoNodeInfoMap()
	want := 0
	for name, item := range c.sc.Nodes {
		if !item.info.Ready() {
			continue
		}
		want++
		node, ok := got[name]
		if !ok {
			tb.Fatalf("node %s is in the cache but not in the snapshot", name)
		}
		if len(node.Tasks) != len(item.info.Tasks) {
			tb.Fatalf("node %s has %d tasks in the snapshot, want %d", name, len(node.Tasks), len(item.info.Tasks))
		}
	}
	if len(got) != want {
		tb.Fatalf("snapshot has %d nodes, want %d", len(got), want)
	}
	if n := len(snapshot.GetFwkNodeInfoList()); n != want {
		tb.Fatalf("snapshot node list has %d nodes, want %d", n, want)
	}
}

// BenchmarkUpdateSnapshot measures the snapshot maintenance an agent-scheduler
// worker does before it schedules a Pod. Every worker owns one snapshot, as
// each worker framework does, and the workers take turns: one scheduling cycle
// changes the cache and then one worker calls UpdateSnapshot. So with w
// workers a snapshot is w cycles behind when it is refreshed.
//
// Only UpdateSnapshot is timed. churn=low changes the cache by one Pod bind
// per cycle, the least a running scheduler does. churn=high adds a node
// resource update every cycle and a node removal plus a node return every
// benchNodeRemoveInterval cycles. bytes/snapshot is the heap one worker
// snapshot keeps alive once it is in sync.
func BenchmarkUpdateSnapshot(b *testing.B) {
	for _, nodes := range []int{100, 500, 1000} {
		for _, workers := range []int{1, 2, 4} {
			for _, churn := range []string{"low", "high"} {
				name := fmt.Sprintf("nodes=%d/workers=%d/churn=%s", nodes, workers, churn)
				b.Run(name, func(b *testing.B) {
					benchmarkUpdateSnapshot(b, nodes, workers, churn == "high")
				})
			}
		}
	}
}

func benchmarkUpdateSnapshot(b *testing.B, nodes, workers int, highChurn bool) {
	cluster := newSnapshotBenchCluster(b, nodes)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	snapshots := make([]*k8sutil.Snapshot, workers)
	for i := range snapshots {
		snapshots[i] = k8sutil.NewEmptySnapshot()
		if err := cluster.sc.UpdateSnapshot(snapshots[i]); err != nil {
			b.Fatalf("initial UpdateSnapshot failed: %v", err)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&after)
	heapPerSnapshot := (float64(after.HeapAlloc) - float64(before.HeapAlloc)) / float64(workers)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cluster.bind(b, i)
		if highChurn {
			cluster.churnNodes(b, i)
		}
		b.StartTimer()

		if err := cluster.sc.UpdateSnapshot(snapshots[i%workers]); err != nil {
			b.Fatalf("UpdateSnapshot failed: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(heapPerSnapshot, "bytes/snapshot")

	for _, snapshot := range snapshots {
		cluster.verify(b, snapshot)
	}
}

// BenchmarkUpdateSnapshotParallel runs the workers at the same time. Every
// worker goroutine takes the next cycle, binds one Pod under the cache write
// lock as the end of a scheduling cycle does, and then refreshes its own
// snapshot under the read lock. The result is wall time per cycle, so it
// includes the bind and the time workers wait for each other on the cache
// lock.
func BenchmarkUpdateSnapshotParallel(b *testing.B) {
	for _, nodes := range []int{100, 500, 1000} {
		for _, workers := range []int{1, 2, 4} {
			name := fmt.Sprintf("nodes=%d/workers=%d", nodes, workers)
			b.Run(name, func(b *testing.B) {
				benchmarkUpdateSnapshotParallel(b, nodes, workers)
			})
		}
	}
}

func benchmarkUpdateSnapshotParallel(b *testing.B, nodes, workers int) {
	cluster := newSnapshotBenchCluster(b, nodes)

	snapshots := make([]*k8sutil.Snapshot, workers)
	for i := range snapshots {
		snapshots[i] = k8sutil.NewEmptySnapshot()
		if err := cluster.sc.UpdateSnapshot(snapshots[i]); err != nil {
			b.Fatalf("initial UpdateSnapshot failed: %v", err)
		}
	}

	var (
		next     atomic.Int64
		bindLock sync.Mutex
		wg       sync.WaitGroup
	)
	total := int64(b.N)

	b.ReportAllocs()
	b.ResetTimer()
	for w := range workers {
		wg.Add(1)
		go func(snapshot *k8sutil.Snapshot) {
			defer wg.Done()
			for {
				cycle := next.Add(1) - 1
				if cycle >= total {
					return
				}
				// bindLock only guards the benchmark's own bookkeeping.
				bindLock.Lock()
				cluster.bind(b, int(cycle))
				bindLock.Unlock()

				if err := cluster.sc.UpdateSnapshot(snapshot); err != nil {
					b.Errorf("UpdateSnapshot failed: %v", err)
					return
				}
			}
		}(snapshots[w])
	}
	wg.Wait()
	b.StopTimer()

	for _, snapshot := range snapshots {
		cluster.verify(b, snapshot)
	}
}
