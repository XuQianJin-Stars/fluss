---
title: "Migrate from ZooKeeper to Raft"
sidebar_position: 6
---

# Migrate from ZooKeeper to Raft

This guide migrates an existing Fluss cluster from the **deprecated ZooKeeper** metadata backend to
the **Raft** backend (default on current builds).

:::warning
ZooKeeper mode is deprecated and will be removed in a future major release. Plan migration before
upgrading to that major.
:::

## Compatibility

| Mode | Status |
|------|--------|
| Raft (default) | Supported |
| ZooKeeper (`zookeeper.address`) | Deprecated but supported |
| Both keys set | Rejected at startup |

Client bootstrap (`bootstrap.servers`) and Admin/DDL APIs are unchanged — only server coordination
config and ops CLI change.

## High-level steps

1. **Export** ZooKeeper metadata with the migration tool (cluster can stay online for the first
   export).
2. **Provision** a Raft coordinator quorum (odd number of voters: 1 / 3 / 5) with empty
   `coordinator.raft.dir`, then `fluss raft bootstrap`.
3. **Import** the export into the Raft leader and run byte-level validation.
4. **Cut over** with either a brief write-fence plus re-export / re-import, or the online
   live-migrate command below, then point clients at the Raft coordinators’
   `bootstrap.servers` / seed nodes.
5. **Decommission** ZooKeeper after soak.

Exact tool subcommands:

```shell
# Export ZK → file (cluster can stay online for the first export)
./bin/fluss zk-to-raft-export --zk-address <zkHost:2181> --output /tmp/zk-export.jsonl

# Import into a bootstrapped Raft quorum
./bin/fluss zk-to-raft-import --input /tmp/zk-export.jsonl --raft-seeds c1:9123,c2:9123,c3:9123

# Validate (compare ZK export vs imported file, or live ZK vs imported)
./bin/fluss zk-to-raft-validate --zk-address <zkHost:2181> --input /tmp/zk-export.jsonl

# Online live-migrate: re-scan {path,mzxid} diffs until quiet, then fence + one final scan
./bin/fluss zk-to-raft-live-migrate --zk-address <zkHost:2181> --raft-seeds c1:9123,c2:9123,c3:9123
```

`zk-to-raft-live-migrate` does **not** use ZooKeeper watches (they coalesce). It repeats a full
re-scan until a round sees no diffs (or `--max-rounds N`, default 32), then asks the operator to
fence ZK writes and does one final scan. Stop-the-world export / import remains the safe fallback.

## Config cut-over

**Before (ZK):**

```yaml
zookeeper.address: zk1:2181,zk2:2181,zk3:2181
zookeeper.path.root: /fluss
```

**After (Raft):**

```yaml
coordinator.seed.nodes: c1:9123,c2:9123,c3:9123
coordinator.raft.voters: c1:9124,c2:9124,c3:9124
coordinator.raft.dir: /data/fluss/raft
# remove zookeeper.address
```

## Rollback

Keep the ZooKeeper ensemble and a final export until the Raft cluster has soaked. To roll back:
stop Raft coordinators, restore previous `server.yaml` with `zookeeper.address`, and restart. Do
**not** dual-write; never set both backends at once.

## Ops after migration

- Prefer `fluss raft status [--watch]` / `leader` for quorum health.
- Scale voters with `add-voter` / `remove-voter` (one change at a time; refuse unsafe removes).
- Quorum loss: `fluss raft unsafe-recover --i-understand-data-loss` on the most up-to-date survivor
  only (accepts possible loss of unreplicated commits).

More detail: design doc `zookeeper-removal-design.md` §6 / §9.
