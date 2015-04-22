// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-fsnotify/fsnotify"
	"github.com/golang/glog"

	"github.com/prometheus/prometheus/config"
	pb "github.com/prometheus/prometheus/config/generated"
)

type FileDiscovery struct {
	sync.RWMutex

	paths   []string
	watcher *fsnotify.Watcher
	done    chan struct{}

	// lastRefresh stores which files were found during the last refresh
	// and how many target groups they contained.
	// This is used to detect deleted target groups.
	lastRefresh map[string]int
}

type FileDiscoveryConfig struct {
	Files []string
}

func NewFileDiscovery(cfg *FileDiscoveryConfig) (*FileDiscovery, error) {
	fd := &FileDiscovery{
		paths: files,
		done:  make(chan struct{}),
	}
	return fd, nil
}

// Sources implements the TargetProvider interface.
func (fd *FileDiscovery) Sources() []string {
	return fd.listFiles()
}

// listFiles returns a list of all files that match the configured patterns.
func (fd *FileDiscovery) listFiles() ([]string, error) {
	var paths []string
	for _, p := range fd.paths {
		files, err := filepath.Glob(p)
		if err != nil {
			return nil, err
		}
		srcs = append(srcs, files...)
	}
	return paths, nil
}

// watchFiles sets watches on all full paths or directories that were configured for
// this file discovery.
func (fd *FileDiscovery) watchFiles() {
	if fd.watcher == nil {
		glog.Error("No watcher configured")
		return
	}
	for _, p := range fd.paths {
		p = strings.Split(p, "*")[0]

		if err := fd.watcher.Add(p); err != nil {
			glog.Errorf("Error add file watch for %q: %s", p, err)
		}
	}
}

// Run implements the TargetProvider interface.
func (fd *FileDiscovery) Run(ch chan<- *config.TargetGroup) {
	defer close(ch)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		glog.Errorf("Error creating file watcher: %s", err)
		return
	}
	fd.watcher = watcher

	fd.refresh(ch)

	for {
		// Stopping has priority over refreshing. Thus we wrap the actual select
		// clause to always catch done signals.
		select {
		case <-fd.done:
			return
		default:
			select {
			case event := <-fd.watcher.Events:
				// fsnotify sometimes sends a bunch of events without name or operation.
				// It's unclear what they are and why they are sent - filter them out.
				if len(event.Name) == 0 {
					continue
				}
				// Everything but a chmod requires rereading.
				if event.Op^fsnotify.Chmod == 0 {
					continue
				}
				// Changes to a file can spawn various sequences of events with
				// different combinations of operations. For all practical purposes
				// this is inaccurate.
				// The most reliable solution is to reload everything if anything happens.
				fd.refresh(ch)

			case <-time.After(30 * time.Second):
				// Setting a new watch after an update might fail. Make sure we don't loose
				// those files forever.
				fd.refresh(ch)

			case err := <-fd.watcher.Errors:
				if err != nil {
					glog.Errorf("Error on file watch: %s", err)
				}

			case <-fd.done:
				return
			}
		}
	}
}

// refresh reads all files matching the discoveries patterns and sends the respective
// updated target groups through the channel.
func (fd *FileDiscovery) refresh(ch <-chan *config.TargetGroup) {
	ref := map[string]int{}

	for _, f := range fd.listFiles() {
		// We have to perform an initial read.
		tgroups, err := fd.readFile(p)
		if err != nil {
			glog.Errorf("Error reading file %q: ", p, err)
			// Start watching this in spite of an error. It might become a correct
			// file in the future.
		}
		for _, tg := range tgroups {
			up <- tg
		}
		ref[f] = len(tgroups)
	}
	// Send empty updates for sources that disappeared.
	for f, n := range fd.lastRefresh {
		m, ok := ref[f]
		if !ok || n > m {
			for i := m; i < n; i++ {
				ch <- &config.TargetGroup{Source: fileSource(f, i)}
			}
		}
	}
	fd.lastRefresh = ref

	fd.watchFiles()
}

// fileSource returns a source ID for the i-th target group in the file.
func fileSource(filename string, i int) string {
	return fmt.Sprintf("file:%s:%d", filename, i)
}

// Stop implements the TargetProvider interface.
func (fd *FileDiscovery) Stop() {
	fd.watcher.Close()
	fd.done <- struct{}{}
}

// readFile reads a list json or protbuf targets groups from the file, depending on its
// file extension. It returns full configuration target groups.
func (fd *FileDiscovery) readFile(filename string) ([]*config.TargetGroup, error) {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	var targetGroups []*pb.TargetGroup

	switch ext := filepath.Ext(filename); ext {
	case ".json":
		dec := json.NewDecoder(f)
		if err := dec.Decode(&targetGroup); err != nil {
			return nil, err
		}
	case ".pb":
		panic("not implemented")
	default:
		return nil, fmt.Errorf("discovery file must have extension .json or .pb but has %s", ext)
	}

	var groups []*config.TargetGroup

	for i, tg := range targetGroups {
		// TODO(fabxc): consider moving this glue code back to the config package.
		g := &config.TargetGroup{
			Source: fileSource(filename, i),
			Labels: clientmodel.LabelSet{},
		}
		for _, pair := range tg.GetLabels().GetLabel() {
			g.Labels[clientmodel.LabelName(pair.GetName())] = clientmodel.LabelValue(pair.GetValue())
		}
		for _, t := range tg.GetTarget() {
			g.Targets = append(g.Targets, clientmodel.LabelSet{
				clientmodel.AddressLabel: clientmodel.LabelValue(t),
			})
		}
		groups = append(groups, g)
	}
	return groups, nil
}
