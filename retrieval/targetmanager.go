// Copyright 2013 The Prometheus Authors
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

package retrieval

import (
	"sync"

	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval/discovery"
	"github.com/prometheus/prometheus/storage"
)

// A TargetProvider maintains information about target groups. If the a provider's
// first view of existing target groups changes he announces the updated groups
// through a channel.
//
// The TargetProvider is responsible for detecting any change to the target groups
// it's responsible for. Disppearing target groups are to be announced through
// sending an empty TargetGroup with the disappeared source identifier to the channel.
type TargetProvider interface {
	// TargetGroups returns a list of target groups the target provider is
	// aware of. After initialization this method should provide a valid list
	// of target groups immediately.
	TargetGroups() []*config.TargetGroup
	// Run hands a channel to the target provider through which it can send
	// updated target groups.
	Run(chan<- *config.TargetGroup) error
	// Stop terminates any potential computation of the target provider.
	Stop()
}

// TargetManager maintains a set of targets, starts and stops their scraping and
// creates the new targets based on the target groups it receives from various
// target providers.
type TargetManager struct {
	m            sync.Mutex
	globalLabels clientmodel.LabelSet

	targets   map[string][]Target
	providers []TargetProvider

	updates        map[string]chan *config.TargetGroup
	configs        map[string]config.JobConfig
	done           chan bool
	sampleAppender storage.SampleAppender
}

// NewTargetManager creates a new TargetManager based on the given config.
func NewTargetManager(cfg config.Config, sampleAppender storage.SampleAppender) (*TargetManager, error) {
	tm := &TargetManager{
		sampleAppender: sampleAppender,
		targets:        make(map[string][]Target),
		done:           make(chan bool),
	}
	if err := tm.updateConfig(cfg); err != nil {
		return nil, err
	}
	return tm, nil
}

// Run starts background processing to handle target updates.
func (tm *TargetManager) Run() {
	for name, ch := range tm.updates {
		go tm.run(tm.configs[name], ch)
	}
}

// run receives target group updates and handles them in the context of the
// provided scrape config.
func (tm *TargetManager) run(cfg config.JobConfig, ch <-chan *config.TargetGroup) {
	for {
		select {
		case <-tm.done:
			return
		case tg := <-ch:
			glog.Infof("update for %s: %v", cfg.GetName(), tg)
			targets, err := tm.updateTargets(tg, cfg)
			if err != nil {
				glog.Errorf("error updating targets: %s", err)
				continue
			}
			tm.m.Lock()
			tm.targets[tg.Source] = targets
			tm.m.Unlock()
		}
	}
}

// Stop all background processing an
func (tm *TargetManager) Stop() {
	tm.m.Lock()
	defer tm.m.Unlock()

	glog.Info("Stopping target manager...")
	for _, p := range tm.providers {
		p.Stop()
	}
	for _, ch := range tm.updates {
		tm.done <- true
		close(ch)
	}

	var wg sync.WaitGroup
	for _, targets := range tm.targets {
		for _, target := range targets {
			wg.Add(1)
			go func(t Target) {
				glog.V(1).Infof("Stopping scraper for target %s...", t)
				t.StopScraper()
				glog.V(1).Infof("Scraper for target %s stopped.", t)
				wg.Done()
			}(target)
		}
	}
	wg.Wait()

	glog.Info("Target manager stopped.")
}

// updateTargets returns a list of targets derived from the config and target group.
// Targets that are not known to the target manager are created.
func (tm *TargetManager) updateTargets(tgroup *config.TargetGroup, cfg config.JobConfig) ([]Target, error) {
	newTargets, err := tm.targetsFromGroup(tgroup, cfg)
	if err != nil {
		return nil, err
	}

	targets, ok := tm.targets[tgroup.Source]
	if ok {
		for i, tnew := range newTargets {
			var match Target
			for i, told := range targets {
				if tnew.InstanceIdentifier() == told.InstanceIdentifier() {
					match = told
					targets[i] = nil
					break
				}
			}
			// Update the exisiting target and discard the new equivalent.
			// Otherwise start scraping the new target.
			if match != nil {
				match.Update(cfg, tgroup.LabelSet())
				newTargets[i] = match
			} else {
				go tnew.RunScraper(tm.sampleAppender, cfg.ScrapeInterval())
			}
		}
		// Remove all old targets that disappeared. did not have match in the new ones.
		for _, told := range targets {
			if told != nil {
				glog.V(1).Infof("Stopping scraper for target %s...", told)
				told.StopScraper()
				glog.V(1).Infof("Scraper for target %s stopped.", told)
			}
		}
	} else {
		for _, tnew := range newTargets {
			go tnew.RunScraper(tm.sampleAppender, cfg.ScrapeInterval())
		}
	}
	return newTargets, nil
}

// Pools returns the targets currently being scraped bucketed by their job name.
func (tm *TargetManager) Pools() map[string][]Target {
	pools := make(map[string][]Target)

	for _, ts := range tm.targets {
		for _, t := range ts {
			job := string(t.BaseLabels()[clientmodel.JobLabel])
			pools[job] = append(pools[job], t)
		}
	}
	return pools
}

// UpdateConfig reinitializes the target manager based on the given config
// and restarts it. The state of targets that remain with the new target providers
// is maintained.
func (tm *TargetManager) UpdateConfig(cfg config.Config) error {
	if err := tm.updateConfig(cfg); err != nil {
		return err
	}

	tm.Stop()
	tm.Run()

	return nil
}

func (tm *TargetManager) updateConfig(cfg config.Config) error {
	tm.m.Lock()
	defer tm.m.Unlock()

	tm.updates = make(map[string]chan *config.TargetGroup)
	tm.configs = make(map[string]config.JobConfig)

	var providers []TargetProvider
	for _, jcfg := range cfg.Jobs() {
		ch := make(chan *config.TargetGroup)

		tm.updates[jcfg.GetName()] = ch
		tm.configs[jcfg.GetName()] = jcfg

		provs, err := providersFromConfig(jcfg)
		if err != nil {
			return err
		}
		for _, p := range provs {
			tgroups := p.TargetGroups()
			for _, tgroup := range tgroups {
				targets, err := tm.updateTargets(tgroup, jcfg)
				if err != nil {
					return err
				}
				tm.targets[tgroup.Source] = targets
			}
			providers = append(providers, p)
		}
	}
	return nil
}

// targetsFromGroup builds targets based on the given TargetGroup and config.
func (tm *TargetManager) targetsFromGroup(tg *config.TargetGroup, cfg config.JobConfig) ([]Target, error) {
	targets := make([]Target, 0, len(tg.Target))
	for _, host := range tg.GetTarget() {
		t := NewTarget(host, cfg, tg.LabelSet())
		targets = append(targets, t)
	}
	return targets, nil
}

// providersFromConfig returns all TargetProviders configured in cfg.
func providersFromConfig(cfg config.JobConfig) ([]TargetProvider, error) {
	var providers []TargetProvider

	if name := cfg.GetSdName(); name != "" {
		dnsSD := discovery.NewDNSDiscovery(name, cfg.SDRefreshInterval())
		providers = append(providers, dnsSD)
	}

	if tgs := cfg.TargetGroups(); tgs != nil {
		static := NewStaticProvider(cfg.GetName(), tgs)
		providers = append(providers, static)
	}
	return providers, nil
}

// StaticProvider holds a list of target groups that never change.
type StaticProvider struct {
	targetGroups []*config.TargetGroup
}

// NewStaticProvider returns a StaticProvider configured with the given
// target groups.
func NewStaticProvider(name string, groups []*config.TargetGroup) *StaticProvider {
	for _, tg := range groups {
		tg.Source = "static:" + name
	}
	return &StaticProvider{groups}
}

// Run implements the TargetProvider interface.
func (sd *StaticProvider) Run(chan<- *config.TargetGroup) error {
	return nil
}

// Stop implements the TargetProvider interface.
func (sd *StaticProvider) Stop() {}

// TargetGroups returns the provider's target groups.
func (sd *StaticProvider) TargetGroups() []*config.TargetGroup {
	return sd.targetGroups
}
