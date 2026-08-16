---
title: "Deploying with Raft (default)"
sidebar_position: 2
---

# Deploying with Raft (default)

Fluss coordinates cluster membership and stores metadata with an **embedded Raft** quorum
(Apache Ratis) by default. No external ZooKeeper is required.

:::tip
Raft is the recommended and default metadata backend. ZooKeeper mode remains available for
migration but is **deprecated** and will be removed in a future major release. See
[Migrate from ZooKeeper to Raft](install-deploy/migrate-zk-to-raft.md).
:::

## When to use this guide

| Scenario | Guide |
|----------|-------|
| Local single-machine tryout | [Local Cluster](install-deploy/deploying-local-cluster.md) (Raft by default) |
| Multi-node bare metal / VMs | This page + [Distributed Cluster](install-deploy/deploying-distributed-cluster.md) |
| Containers | [Docker](install-deploy/deploying-with-docker.md) (prefer Raft compose) |
| Kubernetes | [Helm](install-deploy/deploying-with-helm.md) |

## Core configuration

Select Raft by setting seed nodes (and Raft voters) and **not** setting `zookeeper.address`:

```yaml
# conf/server.yaml (coordinator)
bind.listeners: FLUSS://0.0.0.0:9123
advertised.listeners: FLUSS://coordinator-1:9123

coordinator.seed.nodes: coordinator-1:9123,coordinator-2:9123,coordinator-3:9123
coordinator.raft.voters: coordinator-1:9124,coordinator-2:9124,coordinator-3:9124
coordinator.raft.node.id: coordinator-1:9124
coordinator.raft.dir: /data/fluss/raft

# Do NOT set zookeeper.address when using Raft
```

TabletServers use the same seed list (no Raft voter role):

```yaml
# conf/server.yaml (tablet server)
tablet-server.id: 0
bind.listeners: FLUSS://0.0.0.0:9123
advertised.listeners: FLUSS://tablet-0:9123
coordinator.seed.nodes: coordinator-1:9123,coordinator-2:9123,coordinator-3:9123
```

Firewall the Raft peer port (`coordinator.raft.port`, default **9124**) to coordinator hosts only.
Enable mutual TLS for production with `coordinator.raft.tls.*` (see server configuration reference).

## Optional gossip discovery

By default coordinators and TabletServers use the full static `coordinator.seed.nodes` list.
Optional SWIM-style TCP gossip can **expand a partial seed list**. It does not replace Raft
membership (`coordinator.raft.voters`) or the bootstrap contact (`coordinator.seed.nodes` still
selects the Raft backend).

```yaml
coordinator.gossip.enabled: true
coordinator.gossip.port: 9125
# coordinator.gossip.interval: 1s
# coordinator.gossip.failure-timeout: 10s
```

Leave gossip off (the default) when the seed list is complete. Firewall `coordinator.gossip.port`
(default **9125**) like other coordinator ports.

## Bootstrap and ops CLI

On each new Raft data directory, format once before the first start:

```shell
./bin/fluss raft bootstrap --config conf
```

Useful commands:

```shell
./bin/fluss raft status --coordinator coordinator-1:9123
./bin/fluss raft status --coordinator coordinator-1:9123 --watch
./bin/fluss raft leader --coordinator coordinator-1:9123
./bin/fluss raft add-voter <host:raftPort> --coordinator coordinator-1:9123
./bin/fluss raft remove-voter <serverId> --coordinator coordinator-1:9123
```

`raft status --watch` polls until interrupted (default 2s; `--interval <ms>`). `--json` works with or without `--watch`.

Quorum-loss recovery (offline, explicit operator action only):

```shell
./bin/fluss raft unsafe-recover --i-understand-data-loss --config conf
```

## Single-node (dev)

```yaml
coordinator.seed.nodes: localhost:9123
coordinator.raft.voters: localhost:9124
coordinator.raft.dir: /tmp/fluss-raft
```

Then `./bin/fluss raft bootstrap --config conf` and `./bin/local-cluster.sh start`.

## Feedback

Preview / GA feedback: open a [GitHub issue](https://github.com/apache/fluss/issues) or write to the
Apache Fluss developer mailing list. Please tag issues with `raft` / `zookeeper-removal`.
