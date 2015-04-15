package discovery

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/config"
)

const (
	resolvConf = "/etc/resolv.conf"

	DNSSourcePrefix = "dns"

	// Constants for instrumentation.
	namespace = "prometheus"
	interval  = "interval"
)

var (
	dnsSDLookupsCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dns_sd_lookups_total",
			Help:      "The number of DNS-SD lookups.",
		})
	dnsSDLookupFailuresCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dns_sd_lookup_failures_total",
			Help:      "The number of DNS-SD lookup failures.",
		})
)

func init() {
	prometheus.MustRegister(dnsSDLookupFailuresCount)
	prometheus.MustRegister(dnsSDLookupsCount)
}

type DNSDiscovery struct {
	name string

	done, init  chan bool
	ticker      *time.Ticker
	m           sync.RWMutex
	targetGroup *config.TargetGroup
	update      chan<- *config.TargetGroup
}

func NewDNSDiscovery(name string, refreshInterval time.Duration) *DNSDiscovery {
	return &DNSDiscovery{
		done:   make(chan bool),
		init:   make(chan bool),
		ticker: time.NewTicker(refreshInterval),
		name:   name,
	}
}

func (dd *DNSDiscovery) Run(ch chan<- *config.TargetGroup) error {
	// The ticker will tick for the first time after the the initial
	// interval. We want to start scraping targets right away.
	if err := dd.refresh(ch); err != nil {
		glog.Errorf("error refreshing DNS targets: %s", err)
	}
	dd.init <- true

	go dd.run(ch)
	return nil
}

func (dd *DNSDiscovery) run(ch chan<- *config.TargetGroup) {
	for {
		select {
		case <-dd.ticker.C:
			if err := dd.refresh(ch); err != nil {
				glog.Errorf("error refreshing DNS targets: %s", err)
			}
		case <-dd.done:
			glog.Info("shutting down DNS discovery")
			return
		}
	}
}

func (dd *DNSDiscovery) Stop() {
	dd.ticker.Stop()
	dd.done <- true
}

func (dd *DNSDiscovery) TargetGroups() []*config.TargetGroup {
	// If no targetGroup is set yet, wait until the initial refresh was performed.
	if dd.targetGroup == nil {
		<-dd.init
	}

	dd.m.RLock()
	defer dd.m.RUnlock()

	return []*config.TargetGroup{dd.targetGroup}
}

func (dd *DNSDiscovery) refresh(ch chan<- *config.TargetGroup) error {
	response, err := lookupSRV(dd.name)
	if err != nil {
		return err
	}

	tg := new(config.TargetGroup)
	for _, record := range response.Answer {
		addr, ok := record.(*dns.SRV)
		if !ok {
			glog.Warningf("%q is not a valid SRV record", record)
			continue
		}
		// Remove the final dot from rooted DNS names to make them look more usual.
		addr.Target = strings.TrimRight(addr.Target, ".")

		target := fmt.Sprintf("%s:%d", addr.Target, addr.Port)
		tg.Target = append(tg.Target, target)
	}

	tg.Source = DNSSourcePrefix + ":" + dd.name
	sort.Strings(tg.Target)

	if !tg.Equal(dd.targetGroup) {
		dd.m.Lock()
		dd.targetGroup = tg
		dd.m.Unlock()

		ch <- dd.targetGroup
	}

	return nil
}

func lookupSRV(name string) (*dns.Msg, error) {
	conf, err := dns.ClientConfigFromFile(resolvConf)
	if err != nil {
		return nil, fmt.Errorf("could not load resolv.conf: %s", err)
	}

	client := &dns.Client{}
	response := &dns.Msg{}

	for _, server := range conf.Servers {
		servAddr := net.JoinHostPort(server, conf.Port)
		for _, suffix := range conf.Search {
			response, err = lookup(name, dns.TypeSRV, client, servAddr, suffix, false)
			if err == nil {
				if len(response.Answer) > 0 {
					return response, nil
				}
			} else {
				glog.Warningf("resolving %s.%s failed: %s", name, suffix, err)
			}
		}
		response, err = lookup(name, dns.TypeSRV, client, servAddr, "", false)
		if err == nil {
			return response, nil
		}
	}
	return response, fmt.Errorf("could not resolve %s: No server responded", name)
}

func lookup(name string, queryType uint16, client *dns.Client, servAddr string, suffix string, edns bool) (*dns.Msg, error) {
	msg := &dns.Msg{}
	lname := strings.Join([]string{name, suffix}, ".")
	msg.SetQuestion(dns.Fqdn(lname), queryType)

	if edns {
		opt := &dns.OPT{
			Hdr: dns.RR_Header{
				Name:   ".",
				Rrtype: dns.TypeOPT,
			},
		}
		opt.SetUDPSize(dns.DefaultMsgSize)
		msg.Extra = append(msg.Extra, opt)
	}

	response, _, err := client.Exchange(msg, servAddr)
	if err != nil {
		return nil, err
	}
	if msg.Id != response.Id {
		return nil, fmt.Errorf("DNS ID mismatch, request: %d, response: %d", msg.Id, response.Id)
	}

	if response.MsgHdr.Truncated {
		if client.Net == "tcp" {
			return nil, fmt.Errorf("got truncated message on tcp")
		}
		if edns { // Truncated even though EDNS is used
			client.Net = "tcp"
		}
		return lookup(name, queryType, client, servAddr, suffix, !edns)
	}

	return response, nil
}
