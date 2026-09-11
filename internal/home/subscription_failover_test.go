package home

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func failoverTestClient() *Client {
	return &Client{
		homeCfg: config.HomeConfig{Host: "10.0.0.1", Port: 6379},
		clusterNodes: []clusterNode{
			{IP: "10.0.0.1", Port: 6379},
			{IP: "10.0.0.2", Port: 6379},
		},
		connections: make(map[*homeDispatchConn]struct{}),
	}
}

func TestSubscriptionTimeoutNeedsConsecutiveMisses(t *testing.T) {
	c := failoverTestClient()

	if switched, _ := c.failoverAfterSubscriptionTimeout(); switched {
		t.Fatal("single subscription stall must not flip the cluster node")
	}
	if switched, _ := c.failoverAfterSubscriptionTimeout(); switched {
		t.Fatal("second consecutive stall must not flip the cluster node")
	}
	switched, addr := c.failoverAfterSubscriptionTimeout()
	if !switched {
		t.Fatal("third consecutive stall must flip the cluster node")
	}
	if addr == "" {
		t.Fatal("failover must report the new address")
	}
	if c.homeCfg.Host != "10.0.0.2" {
		t.Fatalf("current node must advance, got %s", c.homeCfg.Host)
	}
}

func TestSubscriptionTimeoutCounterResetsOnSuccess(t *testing.T) {
	c := failoverTestClient()

	if switched, _ := c.failoverAfterSubscriptionTimeout(); switched {
		t.Fatal("single stall must not flip")
	}
	c.resetReconnectFailures()
	if switched, _ := c.failoverAfterSubscriptionTimeout(); switched {
		t.Fatal("counter must reset after a successful receive")
	}
	if switched, _ := c.failoverAfterSubscriptionTimeout(); switched {
		t.Fatal("counter must reset after a successful receive")
	}
	if c.homeCfg.Host != "10.0.0.1" {
		t.Fatalf("node must not move, got %s", c.homeCfg.Host)
	}
}
