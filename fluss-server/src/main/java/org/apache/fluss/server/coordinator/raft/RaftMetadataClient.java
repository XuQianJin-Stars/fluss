/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.raft;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidUpdateVersionException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.metadata.DatabaseSummary;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetCoordinatorRaftStatusResponse;
import org.apache.fluss.rpc.messages.PbRaftVoterState;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.VersionedAcls;
import org.apache.fluss.server.coordinator.LeaderElection;
import org.apache.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.BucketSnapshotJsonSerde;
import org.apache.fluss.server.zk.data.ConfigJsonSerde;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.CoordinatorAddressJsonSerde;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.DatabaseRegistrationJsonSerde;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.LeaderAndIsrJsonSerde;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignmentJsonSerde;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.PartitionRegistrationJsonSerde;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.RebalanceTaskJsonSerde;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandleJsonSerde;
import org.apache.fluss.server.zk.data.ResourceAcl;
import org.apache.fluss.server.zk.data.ResourceAclJsonSerde;
import org.apache.fluss.server.zk.data.ServerTags;
import org.apache.fluss.server.zk.data.ServerTagsJsonSerde;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableAssignmentJsonSerde;
import org.apache.fluss.server.zk.data.TableBucketAndManifest;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TableRegistrationJsonSerde;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistrationJsonSerde;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableJsonSerde;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadata;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadataJsonSerde;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;
import org.apache.fluss.server.zk.data.producer.ProducerOffsetsJsonSerde;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftClientReply;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.retry.RetryPolicies;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.RaftServer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.util.TimeDuration;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Raft-backed {@link MetadataStoreClient} (§5.5.3): the drop-in replacement for {@code
 * ZooKeeperClient} once a coordinator runs on the Raft backend. Writes go through the Raft log
 * ({@link RaftJournalWriter}); reads come from the local {@link RaftMetadataStore}.
 *
 * <p><b>Consistency model (§5.7):</b> all writes and strongly-consistent reads are served ONLY on
 * the leader, guarded by {@link #requireLeader()} (else {@link NotCoordinatorLeaderException}, the
 * "not controller, here's the controller" redirect KRaft uses). A read on the leader is
 * linearizable because {@link RaftJournalWriter#append} resolves only after commit+local-apply, so
 * the leader's store reflects everything it committed. Followers keep an apply-lagged copy used
 * only for warm-standby failover, never for serving client reads — the one exception is {@link
 * #getCoordinatorLeaderAddress()}, which any node may answer so a non-leader can hand back a
 * redirect hint.
 *
 * <p><b>Fencing (§5.1.6):</b> the Raft term subsumes ZooKeeper's {@code (epoch, zkVersion)} CAS —
 * only the current leader can append, so the {@code expectedZkVersion} argument on {@link
 * #updateLeaderAndIsr} is not needed for correctness and is intentionally ignored here (a stale
 * leader's append is rejected by Ratis with {@code NotLeaderException}).
 *
 * <p><b>Thread rule (§5.6):</b> mutating calls do {@code writer.append(...).get()} or {@code
 * writer.appendAll(...).get()} on the caller's (RPC / timer / id-allocation) thread, never on the
 * Ratis apply loop. Multi-key DDL uses {@link #appendAll} so independent log entries share one
 * apply wait (analysis §8.2).
 *
 * <p>This client is constructed with its already-built Raft components (dependency-injected); the
 * factory that bootstraps the full Raft stack from {@code Configuration} and wires it into {@code
 * CoordinatorServer} is the downstream integration step (§7.3.1, tracked as T4-2).
 *
 * <p>See {@code zookeeper-removal-design.md} §5.5.3.
 */
@Internal
public class RaftMetadataClient implements MetadataStoreClient {

    private static final Logger LOG = LoggerFactory.getLogger(RaftMetadataClient.class);

    private static final byte[] EMPTY = new byte[0];

    /**
     * The schema id assigned to a table's first (creation-time) schema; mirrors ZooKeeperClient.
     */
    private static final int DEFAULT_SCHEMA_ID = 1;

    /**
     * Delimiter between a table path and a variable-length suffix (schema id / partition name) in a
     * composite record key. 0x00 cannot appear in a validated table path, so it unambiguously
     * bounds the path prefix and keeps per-table prefix scans collision-free.
     */
    private static final byte KEY_DELIM = 0x00;

    private final RaftJournalWriter writer; // leader-only writes
    private final RaftMetadataStore store; // local read view (leader = linearizable)
    private final RaftLeaderSelector leaderSelector;
    private final RaftSequenceIDCounter tableIdCounter;
    private final RaftSequenceIDCounter partitionIdCounter;
    private final RaftSequenceIDCounter writerIdCounter;

    // When true this is a READ-ONLY follower view over a subscriber-fed store (§5.8): reads are NOT
    // leader-guarded (the follower store is an exact replica of the leader's committed state) and
    // writes/counters/leader-election are unavailable (null). Used to project the tablet-server
    // metadata cache from the stream without an RPC round-trip to the leader for each read.
    private final boolean followerReadOnly;

    // Set only when this client bootstrapped its own Raft stack via create(...) and therefore owns
    // the server lifecycle (closed in close()). Null for dependency-injected instances (tests).
    @Nullable private RaftMetadataServer ownedServer;

    // The state machine of the owned server (set together with ownedServer in create(...)); exposed
    // so the coordinator integration can build the leader-side stream/liveness services that read
    // its applied state + snapshots. Null for dependency-injected instances.
    @Nullable private FlussMetadataStateMachine ownedStateMachine;

    /**
     * Stub voter advertisement of max supported {@code metadata.version} (§5.11). Keyed by Raft
     * peer id ({@code host_port}). Production can later replace this with a registration/heartbeat
     * field; for v1 the leader consults this map before emitting {@link
     * MetadataRecord.Type#UPDATE_METADATA_VERSION}.
     */
    private final Map<String, Integer> voterMaxMetadataVersions = new ConcurrentHashMap<>();

    public RaftMetadataClient(
            RaftJournalWriter writer,
            RaftMetadataStore store,
            RaftLeaderSelector leaderSelector,
            RaftSequenceIDCounter tableIdCounter,
            RaftSequenceIDCounter partitionIdCounter,
            RaftSequenceIDCounter writerIdCounter) {
        this.writer = writer;
        this.store = store;
        this.leaderSelector = leaderSelector;
        this.tableIdCounter = tableIdCounter;
        this.partitionIdCounter = partitionIdCounter;
        this.writerIdCounter = writerIdCounter;
        this.followerReadOnly = false;
    }

    /** Private constructor for a read-only follower view (see {@link #followerReadView}). */
    private RaftMetadataClient(RaftMetadataStore store) {
        this.writer = null;
        this.store = store;
        this.leaderSelector = null;
        this.tableIdCounter = null;
        this.partitionIdCounter = null;
        this.writerIdCounter = null;
        this.followerReadOnly = true;
    }

    /**
     * A READ-ONLY view over a follower store (kept in sync by a {@link MetadataStreamSubscriber},
     * §5.8) that reuses this class's read methods WITHOUT the leader guard, so a tablet server can
     * project its metadata cache from the local replica. Writes and id allocation are unavailable
     * on it.
     */
    public static RaftMetadataClient followerReadView(RaftMetadataStore store) {
        return new RaftMetadataClient(store);
    }

    // ---- bootstrap (§5.5.3 / §7.3.1) ----

    /**
     * Bootstraps a full Raft-backed client from configuration: resolves the voter quorum, builds
     * and starts the embedded {@link RaftMetadataServer}, wires leader election, and assembles the
     * writer + id counters. The returned client OWNS the started server and shuts it down on {@link
     * #close()}.
     *
     * <p>This is the {@code MetadataStoreClientFactory} Raft branch (§7.3.1). It does NOT call
     * {@code startElectLeaderAsync}: leadership notifications flow from the state machine as soon
     * as the server is wired (so {@link #newLeaderSelector} reflects leadership), and the {@code
     * CoordinatorServer} integration later supplies the real {@code initLeaderServices}/{@code
     * cleanupLeaderServices} via {@link RaftLeaderSelector#startElectLeaderAsync}.
     */
    public static RaftMetadataClient create(Configuration conf) throws Exception {
        RaftGroup group = resolveRaftGroup(conf);
        RaftPeerId peerId = resolveLocalPeerId(conf, group);
        return create(peerId, group, conf);
    }

    /**
     * Assembles a Raft-backed client for an explicitly-resolved {@code peerId} and {@code group}.
     * Separated from the config parsing so callers (and tests) can supply a resolved quorum without
     * depending on the voter-string convention.
     */
    public static RaftMetadataClient create(RaftPeerId peerId, RaftGroup group, Configuration conf)
            throws Exception {
        // Production path: durable RocksDB under coordinator.raft.dir/metadata-kv/.
        // Tests that inject a store keep InMemoryMetadataKvStore (followerReadView, unit
        // harnesses).
        MetadataKvStore kvStore = openProductionKvStore(conf);
        RaftMetadataStore store = new RaftMetadataStore(kvStore, new MetadataWatchService());
        FlussMetadataStateMachine stateMachine = new FlussMetadataStateMachine(store);
        RaftMetadataServer server = null;
        try {
            server = new RaftMetadataServer(conf, peerId, group, stateMachine);
            long catchUpTimeoutMs =
                    conf.get(ConfigOptions.COORDINATOR_RAFT_CATCHUP_TIMEOUT).toMillis();
            RaftLeaderSelector leaderSelector =
                    new RaftLeaderSelector(
                            peerId.toString(), server, stateMachine, catchUpTimeoutMs);
            // Wire leadership BEFORE start so the notifyLeaderChanged callback is not missed.
            stateMachine.setLeaderSelector(leaderSelector);

            server.start();

            RaftClient raftClient =
                    RaftClient.newBuilder()
                            .setClientId(ClientId.randomId())
                            .setRaftGroup(group)
                            .setProperties(server.buildRaftProperties())
                            .build();
            RaftJournalWriter writer =
                    new RaftJournalWriter(
                            raftClient,
                            stateMachine,
                            conf.get(ConfigOptions.COORDINATOR_RAFT_APPEND_CHUNK_SIZE));
            long blockSize = conf.get(ConfigOptions.COORDINATOR_RAFT_ID_BLOCK_SIZE);
            RaftMetadataClient client =
                    new RaftMetadataClient(
                            writer,
                            store,
                            leaderSelector,
                            new RaftSequenceIDCounter("table", writer, store, blockSize),
                            new RaftSequenceIDCounter("partition", writer, store, blockSize),
                            new RaftSequenceIDCounter("writer", writer, store, blockSize));
            client.ownedServer = server;
            client.ownedStateMachine = stateMachine;
            // Advertise this binary's max supported metadata.version for the §5.11 feature gate.
            client.advertiseVoterMaxMetadataVersion(
                    peerId.toString(), MetadataVersions.CURRENT_METADATA_VERSION);
            return client;
        } catch (Exception e) {
            if (server != null) {
                try {
                    server.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            try {
                kvStore.close();
            } catch (Exception closeEx) {
                e.addSuppressed(closeEx);
            }
            throw e;
        }
    }

    /**
     * Opens the production RocksDB metadata KvStore under {@code coordinator.raft.dir}/{@link
     * RocksDBMetadataKvStore#SUBDIR}.
     */
    private static MetadataKvStore openProductionKvStore(Configuration conf) throws IOException {
        String raftDir = conf.getOptional(ConfigOptions.COORDINATOR_RAFT_DIR).orElse(null);
        if (raftDir == null || raftDir.trim().isEmpty()) {
            throw new IllegalConfigurationException(
                    "coordinator.raft.dir is required for the Raft metadata KvStore.");
        }
        return RocksDBMetadataKvStore.open(new File(raftDir.trim()));
    }

    /**
     * Resolves the coordinator Raft quorum from {@link ConfigOptions#COORDINATOR_RAFT_VOTERS}.
     *
     * <p><b>Peer-id convention:</b> a voter's {@link RaftPeerId} is derived from its address as
     * {@code host_port} (the {@code ':'} replaced by {@code '_'}), while the {@link RaftPeer} keeps
     * the real {@code host:port} transport address. {@code coordinator.raft.node.id} names the
     * local node's {@code host:port} (see {@link #resolveLocalPeerId}).
     */
    private static RaftGroup resolveRaftGroup(Configuration conf) {
        return resolveGroupForVoters(
                conf.getOptional(ConfigOptions.COORDINATOR_RAFT_VOTERS).orElse(null));
    }

    /**
     * Builds the Raft group for a {@code host:port[,host:port...]} voter list, using the same
     * peer-id scheme as {@link #peerIdForAddress}. Exposed so callers that start bare {@link
     * RaftMetadataServer} peers (multi-voter tests and benchmarks) agree with this client on peer
     * identity — a mismatch means no election ever completes.
     */
    public static RaftGroup resolveGroupForVoters(@Nullable String votersCsv) {
        if (votersCsv == null || votersCsv.trim().isEmpty()) {
            throw new IllegalConfigurationException(
                    "coordinator.raft.voters is required for the Raft metadata backend.");
        }
        List<RaftPeer> peers = new ArrayList<>();
        for (String entry : votersCsv.split(",")) {
            String address = entry.trim();
            if (address.isEmpty()) {
                continue;
            }
            peers.add(
                    RaftPeer.newBuilder()
                            .setId(peerIdForAddress(address))
                            .setAddress(address)
                            .build());
        }
        if (peers.isEmpty()) {
            throw new IllegalConfigurationException(
                    "coordinator.raft.voters did not contain any host:port entries.");
        }
        return RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peers);
    }

    /**
     * Determines which voter in {@code group} is the local node (see {@link #resolveRaftGroup}).
     * {@code coordinator.raft.node.id} is the local node's {@code host:port} (or the equivalent
     * {@code host_port} id); a single-voter quorum needs no explicit id.
     */
    private static RaftPeerId resolveLocalPeerId(Configuration conf, RaftGroup group) {
        String nodeId = conf.getOptional(ConfigOptions.COORDINATOR_RAFT_NODE_ID).orElse(null);
        if (nodeId != null && !nodeId.trim().isEmpty()) {
            // Accept either the host:port address form or the already-derived host_port id form.
            RaftPeerId candidate = peerIdForAddress(nodeId.trim());
            if (group.getPeer(candidate) != null) {
                return candidate;
            }
            throw new IllegalConfigurationException(
                    "coordinator.raft.node.id ("
                            + nodeId
                            + ") does not match any coordinator.raft.voters entry; it must be the "
                            + "local node's host:port.");
        }
        if (group.getPeers().size() == 1) {
            return group.getPeers().iterator().next().getId();
        }
        throw new IllegalConfigurationException(
                "coordinator.raft.node.id must be set (to the local node's host:port, matching one "
                        + "of the coordinator.raft.voters entries) when the voter set has more than "
                        + "one member.");
    }

    /**
     * Derives a {@link RaftPeerId} from an address: {@code host:port -> host_port}. Idempotent for
     * an input that is already in {@code host_port} form (no {@code ':'}).
     */
    @VisibleForTesting
    public static RaftPeerId peerIdForAddress(String address) {
        return RaftPeerId.valueOf(address.trim().replace(':', '_'));
    }

    /**
     * Adds a voter via Ratis single-server membership change ({@code setConfiguration}). Leader
     * only. See zookeeper-removal-design.md §5.10. The Ratis admin RPC is bounded by {@link
     * ConfigOptions#COORDINATOR_RAFT_CATCHUP_TIMEOUT} so a peer that never catches up fails instead
     * of retrying forever.
     *
     * @param address {@code host:raftPort} of the new voter
     * @return the resulting voter set
     */
    public List<PbRaftVoterState> addVoter(String address) throws Exception {
        requireLeader();
        checkNotNull(address, "address");
        String trimmed = address.trim();
        if (trimmed.isEmpty() || trimmed.indexOf(':') < 0) {
            throw new InvalidConfigException(
                    "add-voter address must be host:raftPort, got: " + address);
        }
        RaftPeerId newId = peerIdForAddress(trimmed);
        RaftServer.Division division = requireDivision();
        Collection<RaftPeer> current = currentVoters(division);
        for (RaftPeer peer : current) {
            if (peer.getId().equals(newId)) {
                throw new InvalidConfigException("Voter already present: " + trimmed);
            }
        }
        List<RaftPeer> next = new ArrayList<>(current);
        next.add(RaftPeer.newBuilder().setId(newId).setAddress(trimmed).build());
        RaftClientReply reply = setConfigurationBounded(next);
        if (!reply.isSuccess()) {
            throw reply.getException() != null
                    ? reply.getException()
                    : new InvalidConfigException("setConfiguration(add) failed for " + trimmed);
        }
        LOG.info("Added Raft voter {} (peer id {}); new size={}", trimmed, newId, next.size());
        return listVoters();
    }

    /**
     * Transfers Raft leadership to {@code targetAddress} ({@code host:raftPort}). The current node
     * must be leader; both processes stay up (the former leader becomes a follower). Used by HA
     * tests as the Raft analog of ZooKeeper session expiry.
     *
     * @param targetAddress {@code host:raftPort} of the follower that should become leader
     */
    @VisibleForTesting
    public void transferLeadership(String targetAddress) throws Exception {
        requireLeader();
        checkNotNull(targetAddress, "targetAddress");
        String trimmed = targetAddress.trim();
        if (trimmed.isEmpty() || trimmed.indexOf(':') < 0) {
            throw new InvalidConfigException(
                    "transfer-leadership address must be host:raftPort, got: " + targetAddress);
        }
        RaftPeerId newLeader = peerIdForAddress(trimmed);
        RaftServer.Division division = requireDivision();
        Collection<RaftPeer> current = currentVoters(division);
        int maxPriority = 0;
        for (RaftPeer peer : current) {
            if (peer.getPriority() > maxPriority) {
                maxPriority = peer.getPriority();
            }
        }
        List<RaftPeer> bumped = new ArrayList<>(current.size());
        for (RaftPeer peer : current) {
            if (peer.getId().equals(newLeader)) {
                bumped.add(RaftPeer.newBuilder(peer).setPriority(maxPriority + 1).build());
            } else {
                bumped.add(peer);
            }
        }
        // Ratis transferLeadership requires the target to have uniquely highest priority.
        RaftClientReply confReply = setConfigurationBounded(bumped);
        if (!confReply.isSuccess()) {
            throw confReply.getException() != null
                    ? confReply.getException()
                    : new IOException(
                            "setConfiguration before transferLeadership to "
                                    + trimmed
                                    + " failed: "
                                    + confReply);
        }
        RaftMetadataServer server = requireOwned(ownedServer, "owned Raft server");
        long boundMs =
                server.getConf().get(ConfigOptions.COORDINATOR_RAFT_CATCHUP_TIMEOUT).toMillis();
        if (boundMs <= 0L) {
            boundMs = 30_000L;
        }
        long rpcMs = Math.min(boundMs, 3_000L);
        int maxAttempts = Math.max(1, (int) (boundMs / rpcMs));
        RaftProperties props = server.buildRaftProperties();
        RaftClientConfigKeys.Rpc.setRequestTimeout(
                props, TimeDuration.valueOf(rpcMs, TimeUnit.MILLISECONDS));
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, bumped);
        IOException last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try (RaftClient admin =
                    RaftClient.newBuilder()
                            .setClientId(ClientId.randomId())
                            .setRaftGroup(group)
                            .setLeaderId(division.getId())
                            .setRetryPolicy(
                                    RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                            maxAttempts,
                                            TimeDuration.valueOf(100L, TimeUnit.MILLISECONDS)))
                            .setProperties(props)
                            .build()) {
                RaftClientReply reply = admin.admin().transferLeadership(newLeader, boundMs);
                if (reply.isSuccess()) {
                    LOG.info("Transferred Raft leadership to {} (peer id {})", trimmed, newLeader);
                    return;
                }
                last =
                        reply.getException() != null
                                ? reply.getException()
                                : new IOException(
                                        "transferLeadership to " + trimmed + " failed: " + reply);
                String msg = last.getMessage();
                if (msg == null || !msg.contains("reconfiguration in progress")) {
                    throw last;
                }
            }
            Thread.sleep(100L);
        }
        throw last != null ? last : new IOException("transferLeadership to " + trimmed + " failed");
    }

    /**
     * Removes a voter via Ratis single-server membership change. Leader only. Refuses a removal
     * that would drop the live set below majority of the resulting voter set (§5.10).
     *
     * @param serverId peer id ({@code host_port}) or address ({@code host:port})
     * @return the resulting voter set
     */
    public List<PbRaftVoterState> removeVoter(String serverId) throws Exception {
        requireLeader();
        checkNotNull(serverId, "serverId");
        String trimmed = serverId.trim();
        if (trimmed.isEmpty()) {
            throw new InvalidConfigException("remove-voter server id must not be empty");
        }
        RaftPeerId removeId = peerIdForAddress(trimmed);
        RaftServer.Division division = requireDivision();
        Collection<RaftPeer> current = currentVoters(division);
        RaftPeer toRemove = null;
        for (RaftPeer peer : current) {
            if (peer.getId().equals(removeId)
                    || trimmed.equals(peer.getAddress())
                    || peer.getId().toString().equals(trimmed)) {
                toRemove = peer;
                break;
            }
        }
        if (toRemove == null) {
            throw new InvalidConfigException("No such Raft voter: " + trimmed);
        }
        validateRemoveQuorumSafe(current, toRemove.getId(), liveVoterIds(division));
        List<RaftPeer> next = new ArrayList<>();
        for (RaftPeer peer : current) {
            if (!peer.getId().equals(toRemove.getId())) {
                next.add(peer);
            }
        }
        RaftClientReply reply = setConfigurationBounded(next);
        if (!reply.isSuccess()) {
            throw reply.getException() != null
                    ? reply.getException()
                    : new InvalidConfigException("setConfiguration(remove) failed for " + trimmed);
        }
        LOG.info(
                "Removed Raft voter {} (peer id {}); new size={}",
                toRemove.getAddress(),
                toRemove.getId(),
                next.size());
        return listVoters();
    }

    /**
     * Applies a Ratis membership change with a bounded client RPC/retry budget. The journal {@link
     * RaftClient} defaults to {@code retryForeverNoSleep}, which can hang {@code setConfiguration}
     * forever when a new voter never catches up.
     */
    private RaftClientReply setConfigurationBounded(List<RaftPeer> next) throws Exception {
        RaftMetadataServer server = requireOwned(ownedServer, "owned Raft server");
        RaftServer.Division division = requireDivision();
        long boundMs =
                server.getConf().get(ConfigOptions.COORDINATOR_RAFT_CATCHUP_TIMEOUT).toMillis();
        if (boundMs <= 0L) {
            boundMs = 30_000L;
        }
        long rpcMs = Math.min(boundMs, 3_000L);
        int maxAttempts = Math.max(1, (int) (boundMs / rpcMs));
        RaftProperties props = server.buildRaftProperties();
        RaftClientConfigKeys.Rpc.setRequestTimeout(
                props, TimeDuration.valueOf(rpcMs, TimeUnit.MILLISECONDS));
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, next);
        try (RaftClient admin =
                RaftClient.newBuilder()
                        .setClientId(ClientId.randomId())
                        .setRaftGroup(group)
                        .setLeaderId(division.getId())
                        .setRetryPolicy(
                                RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                        maxAttempts,
                                        TimeDuration.valueOf(100L, TimeUnit.MILLISECONDS)))
                        .setProperties(props)
                        .build()) {
            return admin.admin().setConfiguration(next);
        }
    }

    /**
     * Quorum-safety check for single-server remove (§5.10): after removal, remaining live voters
     * must still be able to form a majority of the <em>new</em> voter set ({@code ceil(newSize /
     * 2)}). Also refuses removing the last voter.
     */
    @VisibleForTesting
    static void validateRemoveQuorumSafe(
            Collection<RaftPeer> currentVoters, RaftPeerId toRemove, Set<RaftPeerId> liveIds) {
        int currentSize = currentVoters.size();
        if (currentSize <= 1) {
            throw new InvalidConfigException(
                    "Refusing remove-voter: cannot remove the last Raft voter");
        }
        boolean found = false;
        for (RaftPeer peer : currentVoters) {
            if (peer.getId().equals(toRemove)) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new InvalidConfigException("No such Raft voter: " + toRemove);
        }
        int newSize = currentSize - 1;
        int majority = newSize / 2 + 1; // ceil(newSize/2) for positive ints
        int liveRemaining = 0;
        for (RaftPeer peer : currentVoters) {
            if (peer.getId().equals(toRemove)) {
                continue;
            }
            if (liveIds.contains(peer.getId())) {
                liveRemaining++;
            }
        }
        if (liveRemaining < majority) {
            throw new InvalidConfigException(
                    "Refusing remove-voter: remaining live voters ("
                            + liveRemaining
                            + ") would be below majority ("
                            + majority
                            + ") of the new voter set size "
                            + newSize);
        }
    }

    /** Current voting peers from the leader's Raft configuration. */
    public List<PbRaftVoterState> listVoters() throws Exception {
        RaftServer.Division division = requireDivision();
        Collection<RaftPeer> peers = currentVoters(division);
        RaftPeerId leaderId = division.getInfo().getLeaderId();
        Map<RaftPeerId, Long> matchIndexes = matchIndexes(division);
        List<PbRaftVoterState> voters = new ArrayList<>(peers.size());
        for (RaftPeer peer : peers) {
            String role =
                    peer.getId().equals(leaderId)
                            ? "LEADER"
                            : division.getInfo().isCandidate()
                                            && peer.getId().equals(division.getId())
                                    ? "CANDIDATE"
                                    : "FOLLOWER";
            long match =
                    matchIndexes.containsKey(peer.getId())
                            ? matchIndexes.get(peer.getId())
                            : (peer.getId().equals(division.getId())
                                    ? division.getRaftLog().getLastCommittedIndex()
                                    : 0L);
            voters.add(
                    new PbRaftVoterState()
                            .setServerId(stableServerId(peer.getId()))
                            .setAddress(peer.getAddress() != null ? peer.getAddress() : "")
                            .setRole(role)
                            .setMatchIndex(match));
        }
        return voters;
    }

    private RaftServer.Division requireDivision() throws IOException {
        RaftMetadataServer server = requireOwned(ownedServer, "owned Raft server");
        return server.getRaftServer().getDivision(RaftMetadataServer.RAFT_GROUP_ID);
    }

    private static Collection<RaftPeer> currentVoters(RaftServer.Division division) {
        return division.getRaftConf().getCurrentPeers();
    }

    /**
     * Peers considered live for quorum-safety: the local peer when alive, plus any peer present in
     * the leader's commit-info table (Ratis tracks reachable followers there).
     */
    private static Set<RaftPeerId> liveVoterIds(RaftServer.Division division) {
        Set<RaftPeerId> live = new HashSet<>();
        if (division.getInfo().isAlive()) {
            live.add(division.getId());
        }
        for (CommitInfoProto info : division.getCommitInfos()) {
            if (info.hasServer()) {
                live.add(RaftPeerId.valueOf(info.getServer().getId()));
            }
        }
        // If commit infos are empty (e.g. single-node / early), treat all current peers as live so
        // only the last-voter and explicit under-majority cases refuse.
        if (live.size() <= 1) {
            for (RaftPeer peer : currentVoters(division)) {
                live.add(peer.getId());
            }
        }
        return live;
    }

    private static Map<RaftPeerId, Long> matchIndexes(RaftServer.Division division) {
        Map<RaftPeerId, Long> out = new HashMap<>();
        for (CommitInfoProto info : division.getCommitInfos()) {
            if (info.hasServer()) {
                out.put(RaftPeerId.valueOf(info.getServer().getId()), info.getCommitIndex());
            }
        }
        return out;
    }

    /** Stable positive int id for {@link PbRaftVoterState#setServerId} (display / CLI). */
    private static int stableServerId(RaftPeerId peerId) {
        return peerId.toString().hashCode() & 0x7fffffff;
    }

    // ---- metadata.version feature gate (§5.11) ----

    /**
     * Records the max {@code metadata.version} a voter advertises it can apply. Stub for v1 —
     * production can later carry this on registration/heartbeat. The local peer is advertised
     * automatically in {@link #create(RaftPeerId, RaftGroup, Configuration)}.
     *
     * @param voterId Raft peer id ({@code host_port})
     * @param maxSupportedVersion highest version the voter can apply
     */
    public void advertiseVoterMaxMetadataVersion(String voterId, int maxSupportedVersion) {
        checkNotNull(voterId, "voterId");
        if (maxSupportedVersion < 0) {
            throw new IllegalArgumentException(
                    "maxSupportedVersion must be >= 0, got " + maxSupportedVersion);
        }
        voterMaxMetadataVersions.put(voterId, maxSupportedVersion);
        LOG.info("Voter {} advertises max metadata.version={}", voterId, maxSupportedVersion);
    }

    /**
     * Returns a snapshot of advertised voter max versions (peer id → max). For tests and
     * diagnostics.
     */
    @VisibleForTesting
    public Map<String, Integer> voterMaxMetadataVersions() {
        return Collections.unmodifiableMap(new HashMap<>(voterMaxMetadataVersions));
    }

    /**
     * Returns the persisted cluster {@code metadata.version}, or {@link
     * MetadataVersions#UNSET_METADATA_VERSION} if none has been committed yet. Leader-only
     * (linearizable read).
     */
    public int getMetadataVersion() {
        requireLeader();
        byte[] key =
                MetadataKeys.keyOf(
                        new MetadataRecord(
                                MetadataRecord.Type.UPDATE_METADATA_VERSION, null, EMPTY));
        byte[] payload = store.get(key);
        if (payload == null) {
            return MetadataVersions.UNSET_METADATA_VERSION;
        }
        return MetadataVersions.decode(payload);
    }

    /**
     * Bumps the cluster {@code metadata.version} via an {@link
     * MetadataRecord.Type#UPDATE_METADATA_VERSION} append. Leader-only. Refuses when:
     *
     * <ul>
     *   <li>{@code newVersion} is above {@link MetadataVersions#CURRENT_METADATA_VERSION} (this
     *       binary)
     *   <li>{@code newVersion} is below the currently persisted version (no downgrade)
     *   <li>any known voter advertises a max strictly below {@code newVersion}
     * </ul>
     *
     * <p>Idempotent when {@code newVersion} equals the current persisted version.
     */
    public void updateMetadataVersion(int newVersion) throws Exception {
        requireLeader();
        if (newVersion > MetadataVersions.CURRENT_METADATA_VERSION) {
            throw new InvalidUpdateVersionException(
                    "Cannot bump metadata.version to "
                            + newVersion
                            + "; this coordinator only supports up to "
                            + MetadataVersions.CURRENT_METADATA_VERSION);
        }
        if (newVersion < MetadataVersions.UNSET_METADATA_VERSION) {
            throw new InvalidUpdateVersionException(
                    "metadata.version must be >= 0, got " + newVersion);
        }
        int current = getMetadataVersion();
        if (newVersion < current) {
            throw new InvalidUpdateVersionException(
                    "Downgrade of metadata.version from "
                            + current
                            + " to "
                            + newVersion
                            + " is not supported (§5.11); once raised, downgrade requires a "
                            + "metadata downgrade step or is blocked");
        }
        if (newVersion == current) {
            return;
        }
        for (Map.Entry<String, Integer> e : voterMaxMetadataVersions.entrySet()) {
            if (e.getValue() < newVersion) {
                throw new InvalidUpdateVersionException(
                        "Refusing metadata.version bump to "
                                + newVersion
                                + "; voter "
                                + e.getKey()
                                + " only supports up to "
                                + e.getValue());
            }
        }
        append(
                MetadataRecord.Type.UPDATE_METADATA_VERSION,
                null,
                MetadataVersions.encode(newVersion));
        LOG.info("Cluster metadata.version bumped {} -> {}", current, newVersion);
    }

    // ---- coordinator / epoch ----

    @Override
    public void registerCoordinatorServer(CoordinatorAddress coordinatorAddress) {
        // In the Raft backend the set of coordinators IS the Raft voter set
        // (coordinator.raft.voters,
        // Appendix 11.2), not an ephemeral metadata record. Nothing to persist here.
        LOG.debug(
                "registerCoordinatorServer is a no-op on the Raft backend (coordinator membership "
                        + "is the Raft voter set): {}",
                coordinatorAddress.getId());
    }

    @Override
    public void registerCoordinatorLeader(CoordinatorAddress coordinatorAddress) throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        coordinatorAddress, CoordinatorAddressJsonSerde.INSTANCE);
        append(MetadataRecord.Type.REGISTER_COORDINATOR_LEADER, EMPTY, payload);
    }

    @Override
    public void unregisterCoordinatorLeader(CoordinatorAddress coordinatorAddress)
            throws Exception {
        requireLeader();
        append(MetadataRecord.Type.DELETE_COORDINATOR_LEADER, EMPTY, EMPTY);
    }

    @Override
    public Optional<CoordinatorAddress> getCoordinatorLeaderAddress() {
        // NOT leader-guarded (§5.7): a non-leader answers this to hand back a redirect hint. The
        // record is replicated+applied on every node, so a follower's local store is sufficient.
        byte[] value = store.get(coordinatorLeaderKey());
        if (value == null || value.length == 0) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, CoordinatorAddressJsonSerde.INSTANCE));
    }

    /**
     * Returns the coordinator server ids known to the metadata store — on the Raft backend the
     * single CURRENT leader's id (or empty when none is registered), NOT the full voter set.
     *
     * <p>Rationale: {@link #registerCoordinatorServer} is a no-op on Raft (coordinator membership
     * is the Raft voter set, a transport-layer concern keyed by {@code host_port} peer ids), so the
     * only coordinator identity recorded in the store is the leader's {@link CoordinatorAddress}
     * (keyed by the coordinator's own server id). Deriving the list from that record keeps it in
     * the SAME id space as the {@code REGISTER/DELETE_COORDINATOR_LEADER} change events consumed by
     * {@code RaftCoordinatorChangeWatcher} ({@code NewCoordinatorEvent}/{@code
     * DeadCoordinatorEvent} both carry {@code CoordinatorAddress.getId()}), so the coordinator's
     * {@code liveCoordinatorServers} seed (this list) and its incremental add/remove events never
     * diverge. Enumerating the full voter set (a different id space) is a separate concern.
     *
     * <p>NOT leader-guarded, mirroring {@link #getCoordinatorLeaderAddress()}.
     */
    public List<String> getCoordinatorServerList() {
        return getCoordinatorLeaderAddress()
                .map(address -> Collections.singletonList(address.getId()))
                .orElse(Collections.emptyList());
    }

    // ---- tablet server ----

    @Override
    public void registerTabletServer(
            int tabletServerId, TabletServerRegistration tabletServerRegistration)
            throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        tabletServerRegistration, TabletServerRegistrationJsonSerde.INSTANCE);
        append(MetadataRecord.Type.REGISTER_TABLET_SERVER, intToBytes(tabletServerId), payload);
    }

    /**
     * Removes tablet-server membership ({@code FENCE_TABLET_SERVER}). Used for lease expiry and for
     * bounce re-registration (fence then register) so watchers see delete+create like ZooKeeper
     * ephemeral nodes.
     */
    public void fenceTabletServer(int tabletServerId) throws Exception {
        requireLeader();
        append(MetadataRecord.Type.FENCE_TABLET_SERVER, intToBytes(tabletServerId), new byte[0]);
    }

    @Override
    public Optional<TabletServerRegistration> getTabletServer(int tabletServerId) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.REGISTER_TABLET_SERVER,
                                intToBytes(tabletServerId)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(
                JsonSerdeUtils.readValue(value, TabletServerRegistrationJsonSerde.INSTANCE));
    }

    @Override
    public Map<Integer, TabletServerRegistration> getTabletServers(int[] tabletServerIds) {
        requireLeader();
        Map<Integer, TabletServerRegistration> result = new LinkedHashMap<>();
        for (int id : tabletServerIds) {
            getTabletServer(id).ifPresent(reg -> result.put(id, reg));
        }
        return result;
    }

    @Override
    public int[] getSortedTabletServerList() {
        requireLeader();
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLET_SERVER);
        List<byte[][]> entries = store.scanPrefixEntries(prefix);
        List<Integer> ids = new ArrayList<>(entries.size());
        for (byte[][] entry : entries) {
            byte[] key = entry[0];
            // key = 3-byte header + 4-byte tablet server id (see registerTabletServer)
            ids.add(intFromBytes(key, prefix.length));
        }
        return ids.stream().mapToInt(Integer::intValue).sorted().toArray();
    }

    // ---- database (former /metadata/databases/**) ----

    /**
     * Registers a database. The replicated {@link MetadataRecord.Type#REGISTER_DATABASE} record IS
     * the registration (no separate znode), keyed by the database name. Mirrors {@code
     * ZooKeeperClient.registerDatabase}; the caller ({@code MetadataManager}) has already checked
     * existence, so this is an unconditional put (last-write-wins, consistent with the ZK backend
     * where a concurrent create would throw {@code NodeExistsException} which the caller maps to
     * "already exists").
     */
    public void registerDatabase(String database, DatabaseRegistration databaseRegistration)
            throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        databaseRegistration, DatabaseRegistrationJsonSerde.INSTANCE);
        append(MetadataRecord.Type.REGISTER_DATABASE, dbNameBytes(database), payload);
    }

    /** Overwrites a database's registration (property/comment change). */
    public void updateDatabase(String database, DatabaseRegistration databaseRegistration)
            throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        databaseRegistration, DatabaseRegistrationJsonSerde.INSTANCE);
        // REGISTER_DATABASE is a plain put in RaftMetadataStore.apply, so re-appending overwrites.
        append(MetadataRecord.Type.REGISTER_DATABASE, dbNameBytes(database), payload);
    }

    /** Returns the registration for a database, if present. */
    public Optional<DatabaseRegistration> getDatabase(String database) {
        requireLeader();
        byte[] value =
                store.get(keyOf(MetadataRecord.Type.REGISTER_DATABASE, dbNameBytes(database)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, DatabaseRegistrationJsonSerde.INSTANCE));
    }

    /** Deletes a database record. The caller has already verified it is empty (or cascade). */
    public void deleteDatabase(String database) throws Exception {
        requireLeader();
        append(MetadataRecord.Type.DELETE_DATABASE, dbNameBytes(database), EMPTY);
    }

    /** Whether a database record exists. */
    public boolean databaseExists(String database) {
        requireLeader();
        return store.get(keyOf(MetadataRecord.Type.REGISTER_DATABASE, dbNameBytes(database)))
                != null;
    }

    /** Lists all registered database names (recovered from the record keys). */
    public List<String> listDatabases() {
        requireLeader();
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_DATABASE);
        List<byte[][]> entries = store.scanPrefixEntries(prefix);
        List<String> names = new ArrayList<>(entries.size());
        for (byte[][] entry : entries) {
            names.add(stringFromKey(entry[0], prefix.length));
        }
        return names;
    }

    /**
     * Builds per-database summaries for the given names. Unlike the ZooKeeper backend, which reads
     * the znode {@code Stat} (ctime + children count), the Raft backend derives {@code createdTime}
     * from the {@link DatabaseRegistration} itself and the table count from a prefix scan of {@link
     * MetadataRecord.Type#REGISTER_TABLE} records keyed by {@code database + "."}. Only names that
     * currently exist are included (matching the ZK behavior that skips missing entries).
     */
    public List<DatabaseSummary> listDatabaseSummaries(Collection<String> databaseNames) {
        requireLeader();
        byte[] tablePrefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE);
        List<byte[][]> tableEntries = store.scanPrefixEntries(tablePrefix);
        List<DatabaseSummary> summaries = new ArrayList<>();
        for (String databaseName : databaseNames) {
            Optional<DatabaseRegistration> reg = getDatabase(databaseName);
            if (!reg.isPresent()) {
                continue;
            }
            String qualifiedPrefix = databaseName + ".";
            int tableCount = 0;
            for (byte[][] entry : tableEntries) {
                String tablePath = stringFromKey(entry[0], tablePrefix.length);
                if (tablePath.startsWith(qualifiedPrefix)) {
                    tableCount++;
                }
            }
            summaries.add(new DatabaseSummary(databaseName, reg.get().createdTime, tableCount));
        }
        return summaries;
    }

    // ---- table / leaderAndIsr ----

    @Override
    public void registerTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        registerTable(tablePath, tableRegistration, true);
    }

    @Override
    public void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception {
        requireLeader();
        // needCreateNode is a ZooKeeper znode-lifecycle flag; the Raft backend has no separate node
        // to create — the replicated record IS the registration — so the flag is not applicable.
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        tableRegistration, TableRegistrationJsonSerde.INSTANCE);
        append(MetadataRecord.Type.REGISTER_TABLE, tablePathBytes(tablePath), payload);
    }

    @Override
    public Optional<TableRegistration> getTable(TablePath tablePath) {
        requireLeader();
        byte[] value =
                store.get(keyOf(MetadataRecord.Type.REGISTER_TABLE, tablePathBytes(tablePath)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, TableRegistrationJsonSerde.INSTANCE));
    }

    /**
     * Batch variant of {@link #getTable}. Only tables that currently exist are included in the
     * result (matching {@code ZooKeeperClient.getTables}, whose caller decides how to treat missing
     * paths).
     */
    public Map<TablePath, TableRegistration> getTables(Collection<TablePath> tablePaths) {
        requireLeader();
        Map<TablePath, TableRegistration> result = new LinkedHashMap<>();
        for (TablePath tablePath : tablePaths) {
            getTable(tablePath).ifPresent(reg -> result.put(tablePath, reg));
        }
        return result;
    }

    /** Whether a table registration exists (the record IS the registration data, §5.5.3). */
    public boolean tableExist(TablePath tablePath) {
        requireLeader();
        return store.get(keyOf(MetadataRecord.Type.REGISTER_TABLE, tablePathBytes(tablePath)))
                != null;
    }

    /** Lists the table names registered under {@code databaseName}. */
    public List<String> listTables(String databaseName) {
        requireLeader();
        // Prefix = header + "database." so only this database's tables match; the id-less remainder
        // of each key is exactly the table name (TablePath.toString() == database + "." + table).
        byte[] prefix =
                keyOf(
                        MetadataRecord.Type.REGISTER_TABLE,
                        (databaseName + ".").getBytes(StandardCharsets.UTF_8));
        List<String> tables = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            tables.add(stringFromKey(entry[0], prefix.length));
        }
        return tables;
    }

    /** Overwrites a table's registration (property/schema-version change). */
    public void updateTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        tableRegistration, TableRegistrationJsonSerde.INSTANCE);
        // REGISTER_TABLE is a plain put in RaftMetadataStore.apply, so re-appending overwrites.
        append(MetadataRecord.Type.REGISTER_TABLE, tablePathBytes(tablePath), payload);
    }

    /**
     * Registers first schema, optional assignment, and table as independent log entries, waiting
     * once for apply. {@code tableRegistration.tableId} must already have been allocated (and any
     * {@code ALLOCATE_ID_RANGE} applied) by the caller.
     */
    @Override
    public void registerTableSchemaAssignmentAndMetadata(
            TablePath tablePath,
            Schema schema,
            @Nullable TableAssignment tableAssignment,
            TableRegistration tableRegistration)
            throws Exception {
        List<MetadataRecord> records = new ArrayList<>();
        records.add(
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_SCHEMA,
                        schemaKeyBytes(tablePath, DEFAULT_SCHEMA_ID),
                        schema.toJsonBytes()));
        if (tableAssignment != null) {
            records.add(
                    new MetadataRecord(
                            MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT,
                            longToBytes(tableRegistration.tableId),
                            JsonSerdeUtils.writeValueAsBytes(
                                    tableAssignment, TableAssignmentJsonSerde.INSTANCE)));
        }
        records.add(
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        tablePathBytes(tablePath),
                        JsonSerdeUtils.writeValueAsBytes(
                                tableRegistration, TableRegistrationJsonSerde.INSTANCE)));
        appendAll(records);
    }

    /**
     * Deletes a table registration.
     *
     * <p>Also deletes partition and schema registrations under this path first, mirroring {@code
     * ZooKeeperClient#deleteTable}'s {@code deletingChildrenIfNeeded}. Without that cascade, Raft
     * leaves orphan partitions that a subsequent create of the same {@link TablePath} still sees
     * (writers then target the new tableId with stale partition ids → {@code
     * NOT_LEADER_OR_FOLLOWER} forever), and leftover schemas make {@link #getCurrentSchemaId}
     * return the dropped table's latest schema (e.g. after {@code ALTER TABLE ADD COLUMN} + drop +
     * recreate). Deleting children first also lets {@link RaftTableChangeWatcher} emit
     * drop-partition events before the drop-table event.
     */
    public void deleteTable(TablePath tablePath) throws Exception {
        requireLeader();
        // Snapshot names/ids up front — each append mutates the live store. All removals go out as
        // one batch so a table with many partitions/schemas costs one apply wait, not one per key
        // (analysis §8.9.2).
        List<MetadataRecord> records = new ArrayList<>();
        for (String partitionName : new ArrayList<>(partitionEntries(tablePath).keySet())) {
            records.add(
                    new MetadataRecord(
                            MetadataRecord.Type.DELETE_PARTITION,
                            partitionKeyBytes(tablePath, partitionName),
                            EMPTY));
        }
        for (Integer schemaId : new ArrayList<>(schemaIds(tablePath))) {
            records.add(
                    new MetadataRecord(
                            MetadataRecord.Type.DELETE_SCHEMA,
                            schemaKeyBytes(tablePath, schemaId),
                            EMPTY));
        }
        records.add(
                new MetadataRecord(
                        MetadataRecord.Type.DELETE_TABLE, tablePathBytes(tablePath), EMPTY));
        appendAll(records);
    }

    @Override
    public void updateLeaderAndIsr(
            TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion)
            throws Exception {
        requireLeader();
        // expectedZkVersion is intentionally ignored: Raft-term fencing (§5.1.6) replaces the ZK
        // CAS.
        appendRecord(leaderAndIsrRecord(tableBucket, leaderAndIsr));
    }

    /**
     * Registers a bucket's leader-and-isr. ZooKeeper's create-vs-set distinction has no Raft analog
     * — this is a plain put, and the single-writer leader subsumes the {@code expectedZkVersion}
     * CAS (§5.1.6).
     */
    public void registerLeaderAndIsr(
            TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion)
            throws Exception {
        updateLeaderAndIsr(tableBucket, leaderAndIsr, expectedZkVersion);
    }

    /**
     * Batch-registers leader-and-isr for a partitioned table's buckets. ZooKeeper did this in one
     * transaction; on Raft these are independent log entries pipelined into a single apply wait
     * (§8.9.2), not one commit-blocking append per bucket — a 128-bucket table would otherwise cost
     * 128 sequential round-trips on the failover path. A single composite record is a possible
     * future atomicity hardening (§8.2.3 explains why {@code MULTI_OP} is not it).
     */
    public void batchRegisterLeaderAndIsrForTablePartition(
            List<RegisterTableBucketLeadAndIsrInfo> registerList, int expectedZkVersion)
            throws Exception {
        requireLeader();
        if (registerList.isEmpty()) {
            return;
        }
        List<MetadataRecord> records = new ArrayList<>(registerList.size());
        for (RegisterTableBucketLeadAndIsrInfo info : registerList) {
            records.add(leaderAndIsrRecord(info.getTableBucket(), info.getLeaderAndIsr()));
        }
        appendAll(records);
    }

    /**
     * Batch-updates leader-and-isr for the given buckets (pipelined into one apply wait; see {@link
     * #batchRegisterLeaderAndIsrForTablePartition} on atomicity).
     */
    public void batchUpdateLeaderAndIsr(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrList, int expectedZkVersion)
            throws Exception {
        requireLeader();
        if (leaderAndIsrList.isEmpty()) {
            return;
        }
        List<MetadataRecord> records = new ArrayList<>(leaderAndIsrList.size());
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : leaderAndIsrList.entrySet()) {
            records.add(leaderAndIsrRecord(entry.getKey(), entry.getValue()));
        }
        appendAll(records);
    }

    /**
     * Builds an {@code UPDATE_LEADER_AND_ISR} record. The ZooKeeper {@code expectedZkVersion} CAS
     * is subsumed by Raft-term fencing (§5.1.6), so it is intentionally not encoded.
     */
    private MetadataRecord leaderAndIsrRecord(TableBucket tableBucket, LeaderAndIsr leaderAndIsr) {
        return new MetadataRecord(
                MetadataRecord.Type.UPDATE_LEADER_AND_ISR,
                tableBucketBytes(tableBucket),
                JsonSerdeUtils.writeValueAsBytes(leaderAndIsr, LeaderAndIsrJsonSerde.INSTANCE));
    }

    /** Returns a bucket's leader-and-isr, if present. */
    public Optional<LeaderAndIsr> getLeaderAndIsr(TableBucket tableBucket) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.UPDATE_LEADER_AND_ISR,
                                tableBucketBytes(tableBucket)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, LeaderAndIsrJsonSerde.INSTANCE));
    }

    /**
     * Batch variant of {@link #getLeaderAndIsr}; only buckets with a leader-and-isr appear in the
     * result (ZK pipelines these; the Raft store answers each from its committed image).
     */
    public Map<TableBucket, LeaderAndIsr> getLeaderAndIsrs(Collection<TableBucket> tableBuckets) {
        requireLeader();
        Map<TableBucket, LeaderAndIsr> result = new LinkedHashMap<>();
        for (TableBucket tableBucket : tableBuckets) {
            getLeaderAndIsr(tableBucket).ifPresent(lai -> result.put(tableBucket, lai));
        }
        return result;
    }

    // ---- schema (former /metadata/.../schemas/[schemaId]) ----

    /** Registers a table's first schema (schema id {@value #DEFAULT_SCHEMA_ID}). */
    public int registerFirstSchema(TablePath tablePath, Schema schema) throws Exception {
        return registerSchema(tablePath, schema, DEFAULT_SCHEMA_ID);
    }

    /**
     * Registers {@code schema} at {@code schemaId} for {@code tablePath}; returns {@code schemaId}.
     */
    public int registerSchema(TablePath tablePath, Schema schema, int schemaId) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.REGISTER_SCHEMA,
                schemaKeyBytes(tablePath, schemaId),
                schema.toJsonBytes());
        return schemaId;
    }

    /** Returns the schema stored at {@code (tablePath, schemaId)}, if present. */
    public Optional<SchemaInfo> getSchemaById(TablePath tablePath, int schemaId) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.REGISTER_SCHEMA,
                                schemaKeyBytes(tablePath, schemaId)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(new SchemaInfo(Schema.fromJsonBytes(value), schemaId));
    }

    /** Returns the highest registered schema id for {@code tablePath}, or 0 if none. */
    public int getCurrentSchemaId(TablePath tablePath) {
        requireLeader();
        return maxSchemaId(tablePath);
    }

    /**
     * Batch variant of {@link #getSchemaById} at the latest id per table. Only tables that have at
     * least one schema are included in the result (matching {@code
     * ZooKeeperClient.getLatestSchemas}).
     */
    public Map<TablePath, SchemaInfo> getLatestSchemas(Collection<TablePath> tablePaths) {
        requireLeader();
        Map<TablePath, SchemaInfo> result = new LinkedHashMap<>();
        for (TablePath tablePath : tablePaths) {
            int latest = maxSchemaId(tablePath);
            if (latest > 0) {
                getSchemaById(tablePath, latest).ifPresent(info -> result.put(tablePath, info));
            }
        }
        return result;
    }

    /** Scans a table's schema records and returns the max schema id, or 0 if none. */
    private int maxSchemaId(TablePath tablePath) {
        int max = 0;
        for (Integer schemaId : schemaIds(tablePath)) {
            max = Math.max(max, schemaId);
        }
        return max;
    }

    /** Schema ids currently registered under {@code tablePath}. */
    private List<Integer> schemaIds(TablePath tablePath) {
        byte[] prefix = keyOf(MetadataRecord.Type.REGISTER_SCHEMA, schemaTableScanBody(tablePath));
        List<Integer> ids = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            ids.add(intFromBytes(entry[0], prefix.length));
        }
        return ids;
    }

    // ---- partition (former /metadata/.../partitions/[name]) + assignments ----

    /** Returns the set of partition names registered for {@code tablePath}. */
    public Set<String> getPartitions(TablePath tablePath) {
        requireLeader();
        return new HashSet<>(partitionEntries(tablePath).keySet());
    }

    /** Returns the registration for a single partition, if present. */
    public Optional<PartitionRegistration> getPartition(TablePath tablePath, String partitionName) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.REGISTER_PARTITION,
                                partitionKeyBytes(tablePath, partitionName)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(
                JsonSerdeUtils.readValue(value, PartitionRegistrationJsonSerde.INSTANCE));
    }

    /** Returns the number of partitions registered for {@code tablePath}. */
    public int getPartitionNumber(TablePath tablePath) {
        requireLeader();
        return store.scanPrefixEntries(keyOfPartitionScan(tablePath)).size();
    }

    /** Returns every partition's registration for {@code tablePath}, keyed by partition name. */
    public Map<String, PartitionRegistration> getPartitionRegistrations(TablePath tablePath) {
        requireLeader();
        return partitionEntries(tablePath);
    }

    /**
     * Returns the partitions of {@code tablePath} whose resolved spec contains {@code
     * partialPartitionSpec} (mirrors {@code ZooKeeperClient.getPartitionRegistrations} filtered).
     */
    public Map<String, PartitionRegistration> getPartitionRegistrations(
            TablePath tablePath,
            List<String> partitionKeys,
            ResolvedPartitionSpec partialPartitionSpec) {
        requireLeader();
        Map<String, PartitionRegistration> result = new LinkedHashMap<>();
        for (Map.Entry<String, PartitionRegistration> entry :
                partitionEntries(tablePath).entrySet()) {
            ResolvedPartitionSpec resolved =
                    ResolvedPartitionSpec.fromPartitionName(partitionKeys, entry.getKey());
            if (resolved.contains(partialPartitionSpec)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Registers a partition's assignment (keyed by partition id) and its metadata (keyed by name),
     * mirroring {@code ZooKeeperClient.registerPartitionAssignmentAndMetadata}. ZooKeeper does this
     * in one multi-op transaction. Raft sends two independent log entries (assignment, then
     * name-keyed {@code REGISTER_PARTITION}) and waits once for both to apply (analysis §8.2).
     *
     * <p>{@code REGISTER_PARTITION} is still last, so a reader never sees a partition without its
     * assignment. If a later entry fails, Ratis may already have committed a prefix (orphan
     * assignment, same as a crash between today's serial appends).
     */
    public void registerPartitionAssignmentAndMetadata(
            long partitionId,
            String partitionName,
            PartitionAssignment partitionAssignment,
            String remoteDataDir,
            TablePath tablePath,
            long tableId)
            throws Exception {
        byte[] assignmentPayload =
                JsonSerdeUtils.writeValueAsBytes(
                        partitionAssignment, PartitionAssignmentJsonSerde.INSTANCE);
        byte[] registrationPayload =
                JsonSerdeUtils.writeValueAsBytes(
                        new PartitionRegistration(tableId, partitionId, remoteDataDir),
                        PartitionRegistrationJsonSerde.INSTANCE);
        List<MetadataRecord> records = new ArrayList<>(2);
        records.add(
                new MetadataRecord(
                        MetadataRecord.Type.UPDATE_PARTITION_ASSIGNMENT,
                        longToBytes(partitionId),
                        assignmentPayload));
        records.add(
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_PARTITION,
                        partitionKeyBytes(tablePath, partitionName),
                        registrationPayload));
        appendAll(records);
    }

    /** Deletes a partition's metadata record (its assignment is removed separately by id). */
    public void deletePartition(TablePath tablePath, String partitionName) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.DELETE_PARTITION,
                partitionKeyBytes(tablePath, partitionName),
                EMPTY);
    }

    /** Registers (or overwrites) a table's bucket assignment, keyed by table id. */
    public void registerTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        tableAssignment, TableAssignmentJsonSerde.INSTANCE);
        append(MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT, longToBytes(tableId), payload);
    }

    /**
     * Overwrites a table's bucket assignment (rebalance path). The ZooKeeper {@code
     * expectedZkVersion} CAS is subsumed by the single-writer leader term (§5.1.6), so it is
     * ignored — the leader is the only writer, mirroring {@link #updateLeaderAndIsr}.
     */
    public void updateTableAssignment(
            long tableId, TableAssignment tableAssignment, int expectedZkVersion) throws Exception {
        registerTableAssignment(tableId, tableAssignment);
    }

    /** Returns a table's bucket assignment, if present. */
    public Optional<TableAssignment> getTableAssignment(long tableId) {
        requireLeader();
        byte[] value =
                store.get(keyOf(MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT, longToBytes(tableId)));
        if (value == null || value.length == 0) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, TableAssignmentJsonSerde.INSTANCE));
    }

    /** Deletes a table's bucket assignment. */
    public void deleteTableAssignment(long tableId) throws Exception {
        requireLeader();
        // ZK deleteTableAssignment uses deletingChildrenIfNeeded — cascade LeaderAndIsr for
        // non-partitioned table buckets so completeDeleteTable clears bucket state too.
        // Batched into one apply wait instead of one per bucket (analysis §8.9.2).
        Optional<TableAssignment> assignment = getTableAssignment(tableId);
        List<MetadataRecord> records = new ArrayList<>();
        if (assignment.isPresent()) {
            for (Integer bucketId : assignment.get().getBuckets()) {
                records.add(
                        new MetadataRecord(
                                MetadataRecord.Type.DELETE_LEADER_AND_ISR,
                                tableBucketBytes(new TableBucket(tableId, bucketId)),
                                EMPTY));
            }
        }
        records.add(
                new MetadataRecord(
                        MetadataRecord.Type.DELETE_TABLE_ASSIGNMENT, longToBytes(tableId), EMPTY));
        appendAll(records);
    }

    /** Returns a partition's bucket assignment, if present. */
    public Optional<PartitionAssignment> getPartitionAssignment(long partitionId) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.UPDATE_PARTITION_ASSIGNMENT,
                                longToBytes(partitionId)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, PartitionAssignmentJsonSerde.INSTANCE));
    }

    /** Deletes a partition's bucket assignment. */
    public void deletePartitionAssignment(long partitionId) throws Exception {
        requireLeader();
        // ZK deletePartitionAssignment cascades children (leader_isr); mirror that here.
        // Batched into one apply wait instead of one per bucket (analysis §8.9.2).
        Optional<PartitionAssignment> assignment = getPartitionAssignment(partitionId);
        List<MetadataRecord> records = new ArrayList<>();
        if (assignment.isPresent()) {
            long tableId = assignment.get().getTableId();
            for (Integer bucketId : assignment.get().getBuckets()) {
                records.add(
                        new MetadataRecord(
                                MetadataRecord.Type.DELETE_LEADER_AND_ISR,
                                tableBucketBytes(new TableBucket(tableId, partitionId, bucketId)),
                                EMPTY));
            }
        }
        records.add(
                new MetadataRecord(
                        MetadataRecord.Type.DELETE_PARTITION_ASSIGNMENT,
                        longToBytes(partitionId),
                        EMPTY));
        appendAll(records);
    }

    /**
     * Overwrites a partition's bucket assignment (rebalance path). The ZooKeeper {@code
     * expectedZkVersion} CAS is subsumed by the single-writer leader term (§5.1.6), so it is
     * ignored — the leader is the only writer, mirroring {@link #updateLeaderAndIsr}.
     */
    public void updatePartitionAssignment(
            long partitionId, PartitionAssignment partitionAssignment, int expectedZkVersion)
            throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        partitionAssignment, PartitionAssignmentJsonSerde.INSTANCE);
        append(MetadataRecord.Type.UPDATE_PARTITION_ASSIGNMENT, longToBytes(partitionId), payload);
    }

    /** Batch variant of {@link #getTableAssignment}; only assigned tables appear in the result. */
    public Map<Long, TableAssignment> getTablesAssignments(Collection<Long> tableIds) {
        requireLeader();
        Map<Long, TableAssignment> result = new LinkedHashMap<>();
        for (Long tableId : tableIds) {
            getTableAssignment(tableId).ifPresent(a -> result.put(tableId, a));
        }
        return result;
    }

    /**
     * Batch variant of {@link #getPartitionAssignment}; only assigned partitions appear in the
     * result.
     */
    public Map<Long, PartitionAssignment> getPartitionsAssignments(Collection<Long> partitionIds) {
        requireLeader();
        Map<Long, PartitionAssignment> result = new LinkedHashMap<>();
        for (Long partitionId : partitionIds) {
            getPartitionAssignment(partitionId).ifPresent(a -> result.put(partitionId, a));
        }
        return result;
    }

    /**
     * Lists the ids of all tables that currently have a bucket assignment (the Raft equivalent of
     * {@code getChildren(TableIdsZNode.path())}): a prefix scan over {@code
     * UPDATE_TABLE_ASSIGNMENT} records, recovering the 8-byte table id from each key.
     */
    public List<Long> listAssignedTableIds() {
        requireLeader();
        return scanAssignedIds(MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT);
    }

    /**
     * Lists the ids of all partitions that currently have a bucket assignment (the Raft equivalent
     * of {@code getChildren(PartitionIdsZNode.path())}).
     */
    public List<Long> listAssignedPartitionIds() {
        requireLeader();
        return scanAssignedIds(MetadataRecord.Type.UPDATE_PARTITION_ASSIGNMENT);
    }

    private List<Long> scanAssignedIds(MetadataRecord.Type assignmentType) {
        byte[] prefix = MetadataKeys.prefixOf(assignmentType);
        List<Long> ids = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            ids.add(longFromBytes(entry[0], prefix.length));
        }
        return ids;
    }

    /**
     * Returns {@code partitionName -> partitionId} for every partition of each requested table
     * (empty map per table with no partitions). Mirrors {@code
     * ZooKeeperClient.getPartitionNameAndIdsForTables} but derives the mapping from the streamed
     * {@code REGISTER_PARTITION} records rather than ZK children lookups.
     */
    public Map<TablePath, Map<String, Long>> getPartitionNameAndIdsForTables(
            List<TablePath> tablePaths) {
        requireLeader();
        Map<TablePath, Map<String, Long>> result = new LinkedHashMap<>();
        for (TablePath tablePath : tablePaths) {
            Map<String, Long> nameToId = new LinkedHashMap<>();
            for (Map.Entry<String, PartitionRegistration> entry :
                    partitionEntries(tablePath).entrySet()) {
                nameToId.put(entry.getKey(), entry.getValue().getPartitionId());
            }
            result.put(tablePath, nameToId);
        }
        return result;
    }

    /**
     * Returns the cluster's server tags, if any. Persisted as a single global {@code
     * UPSERT_SERVER_TAGS} record (former {@code /metadata/server_tags} znode).
     */
    public Optional<ServerTags> getServerTags() {
        requireLeader();
        byte[] value = store.get(keyOf(MetadataRecord.Type.UPSERT_SERVER_TAGS, EMPTY));
        if (value == null || value.length == 0) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, ServerTagsJsonSerde.INSTANCE));
    }

    /** Registers the cluster's server tags (single global key). */
    public void registerServerTags(ServerTags newServerTags) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.UPSERT_SERVER_TAGS,
                EMPTY,
                JsonSerdeUtils.writeValueAsBytes(newServerTags, ServerTagsJsonSerde.INSTANCE));
    }

    /**
     * Overwrites the cluster's server tags (single global key). Identical to {@link
     * #registerServerTags} on Raft: the single-writer leader subsumes the ZooKeeper
     * create-vs-setData distinction.
     */
    public void updateServerTags(ServerTags newServerTags) throws Exception {
        registerServerTags(newServerTags);
    }

    /** Deletes the cluster's server tags. */
    public void deleteServerTags() throws Exception {
        requireLeader();
        append(MetadataRecord.Type.DELETE_SERVER_TAGS, EMPTY, EMPTY);
    }

    // ---- bucket metadata (assignment + leader-and-isr, for the metadata providers) ----

    /**
     * Returns {@code tableId -> per-bucket metadata} for the given tables (each requested table is
     * present with a possibly-empty list). Mirrors {@code ZooKeeperClient.getBucketMetadataFor
     * Tables}: seed the buckets from each table's assignment, then fold in the leader-and-isr
     * (leader/epoch are null for buckets whose leader is not yet known).
     */
    public Map<Long, List<BucketMetadata>> getBucketMetadataForTables(Collection<Long> tableIds) {
        requireLeader();
        Map<Long, List<BucketMetadata>> result = new LinkedHashMap<>();
        tableIds.forEach(id -> result.put(id, new ArrayList<>()));
        Map<Long, TableAssignment> assignments = getTablesAssignments(tableIds);
        List<TableBucket> buckets = new ArrayList<>();
        assignments.forEach(
                (tableId, assignment) -> {
                    for (Integer bucketId : assignment.getBuckets()) {
                        buckets.add(new TableBucket(tableId, null, bucketId));
                    }
                });
        Map<TableBucket, LeaderAndIsr> leaderAndIsrs = getLeaderAndIsrs(buckets);
        for (TableBucket bucket : buckets) {
            result.get(bucket.getTableId())
                    .add(
                            createBucketMetadata(
                                    leaderAndIsrs, bucket, assignments.get(bucket.getTableId())));
        }
        return result;
    }

    /**
     * Partition variant of {@link #getBucketMetadataForTables}; only partitions with an assignment
     * appear in the result.
     */
    public Map<Long, List<BucketMetadata>> getBucketMetadataForPartitions(
            Collection<Long> partitionIds) {
        requireLeader();
        Map<Long, PartitionAssignment> assignments = getPartitionsAssignments(partitionIds);
        List<TableBucket> buckets = new ArrayList<>();
        assignments.forEach(
                (partitionId, assignment) -> {
                    for (Integer bucketId : assignment.getBuckets()) {
                        buckets.add(
                                new TableBucket(assignment.getTableId(), partitionId, bucketId));
                    }
                });
        Map<TableBucket, LeaderAndIsr> leaderAndIsrs = getLeaderAndIsrs(buckets);
        Map<Long, List<BucketMetadata>> result = new LinkedHashMap<>();
        for (TableBucket bucket : buckets) {
            long partitionId = checkNotNull(bucket.getPartitionId());
            result.computeIfAbsent(partitionId, k -> new ArrayList<>())
                    .add(createBucketMetadata(leaderAndIsrs, bucket, assignments.get(partitionId)));
        }
        return result;
    }

    private static BucketMetadata createBucketMetadata(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrs,
            TableBucket bucket,
            TableAssignment assignment) {
        LeaderAndIsr leaderAndIsr = leaderAndIsrs.get(bucket);
        Integer leader = leaderAndIsr != null ? leaderAndIsr.leader() : null;
        Integer leaderEpoch = leaderAndIsr != null ? leaderAndIsr.leaderEpoch() : null;
        List<Integer> replicas =
                assignment.getBucketAssignments().get(bucket.getBucket()).getReplicas();
        return new BucketMetadata(bucket.getBucket(), leader, leaderEpoch, replicas);
    }

    /**
     * Returns {@code partitionPath -> TablePartition} for the given partition paths (only present
     * partitions appear). Mirrors {@code ZooKeeperClient.getPartitionIds}, assembled here from each
     * partition's registration ({@link #getPartition}).
     */
    public Map<PhysicalTablePath, TablePartition> getPartitionIds(
            Collection<PhysicalTablePath> partitionPaths) {
        requireLeader();
        Map<PhysicalTablePath, TablePartition> result = new LinkedHashMap<>();
        for (PhysicalTablePath partitionPath : partitionPaths) {
            getPartition(
                            partitionPath.getTablePath(),
                            checkNotNull(partitionPath.getPartitionName()))
                    .ifPresent(reg -> result.put(partitionPath, reg.toTablePartition()));
        }
        return result;
    }

    /** Scans a table's REGISTER_PARTITION records into a name -&gt; registration map. */
    private Map<String, PartitionRegistration> partitionEntries(TablePath tablePath) {
        byte[] prefix = keyOfPartitionScan(tablePath);
        Map<String, PartitionRegistration> result = new LinkedHashMap<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            String name = stringFromKey(entry[0], prefix.length);
            result.put(
                    name,
                    JsonSerdeUtils.readValue(entry[1], PartitionRegistrationJsonSerde.INSTANCE));
        }
        return result;
    }

    private static byte[] keyOfPartitionScan(TablePath tablePath) {
        return keyOf(MetadataRecord.Type.REGISTER_PARTITION, partitionTableScanBody(tablePath));
    }

    // ---- kv snapshot lease (former /kv_snapshot_leases/[leaseId]) ----

    /** Registers a new kv-snapshot lease, keyed by lease id. */
    public void registerKvSnapshotLeaseMetadata(
            String leaseId, KvSnapshotLeaseMetadata leaseMetadata) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.UPDATE_KV_SNAPSHOT_LEASE,
                leaseId.getBytes(StandardCharsets.UTF_8),
                JsonSerdeUtils.writeValueAsBytes(
                        leaseMetadata, KvSnapshotLeaseMetadataJsonSerde.INSTANCE));
    }

    /** Overwrites an existing kv-snapshot lease (UPDATE is a plain put in the store). */
    public void updateKvSnapshotLeaseMetadata(String leaseId, KvSnapshotLeaseMetadata leaseMetadata)
            throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.UPDATE_KV_SNAPSHOT_LEASE,
                leaseId.getBytes(StandardCharsets.UTF_8),
                JsonSerdeUtils.writeValueAsBytes(
                        leaseMetadata, KvSnapshotLeaseMetadataJsonSerde.INSTANCE));
    }

    /** Returns a kv-snapshot lease by id, if present. */
    public Optional<KvSnapshotLeaseMetadata> getKvSnapshotLeaseMetadata(String leaseId) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.UPDATE_KV_SNAPSHOT_LEASE,
                                leaseId.getBytes(StandardCharsets.UTF_8)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(
                JsonSerdeUtils.readValue(value, KvSnapshotLeaseMetadataJsonSerde.INSTANCE));
    }

    /** Deletes a kv-snapshot lease by id. */
    public void deleteKvSnapshotLease(String leaseId) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.DELETE_KV_SNAPSHOT_LEASE,
                leaseId.getBytes(StandardCharsets.UTF_8),
                EMPTY);
    }

    /** Lists all kv-snapshot lease ids (the id-less remainder of each lease key). */
    public List<String> getKvSnapshotLeasesList() {
        requireLeader();
        byte[] prefix = keyOf(MetadataRecord.Type.UPDATE_KV_SNAPSHOT_LEASE, EMPTY);
        List<String> ids = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            ids.add(stringFromKey(entry[0], prefix.length));
        }
        return ids;
    }

    // ---- producer offsets (former /producers/[producerId]) ----

    /**
     * Atomically registers a producer's offset snapshot, keyed by producer id; returns {@code
     * false} if one already exists. Mirrors {@code ZooKeeperClient.tryRegisterProducerOffsets}: the
     * ZK backend uses an atomic create-if-absent, the Raft backend does a read-check then append on
     * the single-writer leader (the commit-blocking append serializes the create with any
     * concurrent registration, subsuming ZK's node-exists guard).
     */
    public boolean tryRegisterProducerOffsets(String producerId, ProducerOffsets producerOffsets)
            throws Exception {
        requireLeader();
        // Serialize create-if-absent on the single-writer leader so concurrent registrations of the
        // same producerId cannot all pass a racy read-check (ZK uses atomic create).
        synchronized (this) {
            byte[] key =
                    keyOf(
                            MetadataRecord.Type.REGISTER_PRODUCER_OFFSETS,
                            producerId.getBytes(StandardCharsets.UTF_8));
            if (store.get(key) != null) {
                return false;
            }
            append(
                    MetadataRecord.Type.REGISTER_PRODUCER_OFFSETS,
                    producerId.getBytes(StandardCharsets.UTF_8),
                    JsonSerdeUtils.writeValueAsBytes(
                            producerOffsets, ProducerOffsetsJsonSerde.INSTANCE));
            return true;
        }
    }

    /** Returns a producer's offset snapshot, if present. */
    public Optional<ProducerOffsets> getProducerOffsets(String producerId) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.REGISTER_PRODUCER_OFFSETS,
                                producerId.getBytes(StandardCharsets.UTF_8)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, ProducerOffsetsJsonSerde.INSTANCE));
    }

    /**
     * Returns a producer's offset snapshot with its version. On the Raft backend the version is
     * always {@code 0}: there is no per-record znode version, and the single-writer leader subsumes
     * the optimistic-concurrency control the version enables on ZK (see {@link
     * #deleteProducerSnapshotIfVersion} and the analogous {@code updateLeaderAndIsr} decision).
     */
    public Optional<Tuple2<ProducerOffsets, Integer>> getProducerOffsetsWithVersion(
            String producerId) {
        return getProducerOffsets(producerId).map(offsets -> Tuple2.of(offsets, 0));
    }

    /** Deletes a producer's offset snapshot. */
    public void deleteProducerOffsets(String producerId) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.DELETE_PRODUCER_OFFSETS,
                producerId.getBytes(StandardCharsets.UTF_8),
                EMPTY);
    }

    /**
     * Deletes a producer's offset snapshot only when {@code expectedVersion} matches the Raft-side
     * version (always {@code 0} today). Returns {@code true} if deleted or already absent; {@code
     * false} on version mismatch — mirrors ZK conditional delete.
     */
    public boolean deleteProducerSnapshotIfVersion(String producerId, int expectedVersion)
            throws Exception {
        requireLeader();
        synchronized (this) {
            Optional<Tuple2<ProducerOffsets, Integer>> current =
                    getProducerOffsetsWithVersion(producerId);
            if (!current.isPresent()) {
                // Already gone — same success semantics as ZK NoNode on conditional delete.
                return true;
            }
            if (current.get().f1 != expectedVersion) {
                return false;
            }
            deleteProducerOffsets(producerId);
            return true;
        }
    }

    /** Lists all producer ids with a registered offset snapshot. */
    public List<String> listProducerIds() {
        requireLeader();
        byte[] prefix = keyOf(MetadataRecord.Type.REGISTER_PRODUCER_OFFSETS, EMPTY);
        List<String> ids = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            ids.add(stringFromKey(entry[0], prefix.length));
        }
        return ids;
    }

    // ---- dynamic entity config (former /config/server/global), single global key ----

    /** Returns the dynamic server entity config (empty mutable map if never set). */
    public Map<String, String> fetchEntityConfig() {
        requireLeader();
        byte[] value = store.get(keyOf(MetadataRecord.Type.UPSERT_ENTITY_CONFIG, EMPTY));
        if (value == null) {
            return new HashMap<>();
        }
        return JsonSerdeUtils.readValue(value, ConfigJsonSerde.INSTANCE);
    }

    /** Overwrites the dynamic server entity config (single global key). */
    public void upsertEntityConfigs(Map<String, String> configs) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.UPSERT_ENTITY_CONFIG,
                EMPTY,
                JsonSerdeUtils.writeValueAsBytes(configs, ConfigJsonSerde.INSTANCE));
    }

    /** Alias of {@link #upsertEntityConfigs} used by the dynamic-config manager. */
    public void upsertServerEntityConfig(Map<String, String> configs) throws Exception {
        upsertEntityConfigs(configs);
    }

    // ---- rebalance task (former /cluster/rebalance), single global key ----

    /** Returns the current rebalance task, if any. */
    public Optional<RebalanceTask> getRebalanceTask() {
        requireLeader();
        byte[] value = store.get(keyOf(MetadataRecord.Type.UPSERT_REBALANCE_TASK, EMPTY));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, RebalanceTaskJsonSerde.INSTANCE));
    }

    /** Overwrites the current rebalance task (single global key). */
    public void registerRebalanceTask(RebalanceTask rebalanceTask) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.UPSERT_REBALANCE_TASK,
                EMPTY,
                JsonSerdeUtils.writeValueAsBytes(rebalanceTask, RebalanceTaskJsonSerde.INSTANCE));
    }

    /** Deletes the current rebalance task (single global key). */
    public void deleteRebalanceTask() throws Exception {
        requireLeader();
        append(MetadataRecord.Type.DELETE_REBALANCE_TASK, EMPTY, EMPTY);
    }

    // ---- resource acl (former /acl/[resourceType]/[resourceName]) ----

    /** Creates a resource's ACL entries (Raft: a plain put, create-or-overwrite by key). */
    public void createResourceAcl(Resource resource, Set<AccessControlEntry> accessControlEntries)
            throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.UPSERT_RESOURCE_ACL,
                resourceAclKeyBody(resource),
                JsonSerdeUtils.writeValueAsBytes(
                        new ResourceAcl(accessControlEntries), ResourceAclJsonSerde.INSTANCE));
    }

    /**
     * Overwrites a resource's ACL entries, ignoring {@code expectedVersion} (the single-writer
     * leader subsumes the ZK CAS, as with {@code updateLeaderAndIsr}). Returns 0 — Raft has no
     * per-record version.
     */
    public int updateResourceAcl(
            Resource resource, Set<AccessControlEntry> accessControlEntries, int expectedVersion)
            throws Exception {
        createResourceAcl(resource, accessControlEntries);
        return 0;
    }

    /**
     * Returns a resource's versioned ACLs — version {@code 0} when present, {@code
     * ZooKeeperClient.UNKNOWN_VERSION} when absent (so {@code VersionedAcls.exists()} matches the
     * ZK semantics).
     */
    public VersionedAcls getResourceAclWithVersion(Resource resource) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.UPSERT_RESOURCE_ACL,
                                resourceAclKeyBody(resource)));
        if (value == null) {
            return new VersionedAcls(ZooKeeperClient.UNKNOWN_VERSION, Collections.emptySet());
        }
        ResourceAcl acl = JsonSerdeUtils.readValue(value, ResourceAclJsonSerde.INSTANCE);
        return new VersionedAcls(0, acl.getEntries());
    }

    /** Deletes a resource's ACLs, ignoring {@code zkVersion} (Raft subsumes the CAS). */
    public void conditionalDeleteResourceAcl(Resource resource, int zkVersion) throws Exception {
        requireLeader();
        append(MetadataRecord.Type.DELETE_RESOURCE_ACL, resourceAclKeyBody(resource), EMPTY);
    }

    /** Lists the names of resources of the given type that have ACLs. */
    public List<String> listResourcesByType(ResourceType resourceType) {
        requireLeader();
        byte[] prefix = resourceAclScanPrefix(resourceType);
        List<String> names = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            names.add(stringFromKey(entry[0], prefix.length));
        }
        return names;
    }

    /**
     * No-op on the Raft backend: the ACL record append IS the notification (subscribers observe it
     * via the metadata stream), so there is no separate change-notification node to create.
     */
    public void insertAclChangeNotification(Resource resource) {
        requireLeader();
    }

    /** ACL key body: 1-byte resource-type code + UTF-8 resource name. */
    private static byte[] resourceAclKeyBody(Resource resource) {
        byte[] name = resource.getName().getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + name.length)
                .put(resource.getType().getCode())
                .put(name)
                .array();
    }

    /** Scan prefix bounding one resource type: record header + 1-byte type code. */
    private static byte[] resourceAclScanPrefix(ResourceType resourceType) {
        byte[] header = MetadataKeys.prefixOf(MetadataRecord.Type.UPSERT_RESOURCE_ACL);
        return ByteBuffer.allocate(header.length + 1)
                .put(header)
                .put(resourceType.getCode())
                .array();
    }

    // ---- remote log manifest (former .../buckets/[id]/remote_logs) ----

    /** Upserts a bucket's current remote log manifest handle (keyed by table bucket). */
    public void upsertRemoteLogManifestHandle(
            TableBucket tableBucket, RemoteLogManifestHandle remoteLogManifestHandle)
            throws Exception {
        requireLeader();
        // UPSERT is a plain put in RaftMetadataStore.apply, so it creates or overwrites uniformly
        // (the ZK backend branches on existence, but the Raft record is idempotent by key).
        append(
                MetadataRecord.Type.UPSERT_REMOTE_LOG_MANIFEST,
                tableBucketBytes(tableBucket),
                JsonSerdeUtils.writeValueAsBytes(
                        remoteLogManifestHandle, RemoteLogManifestHandleJsonSerde.INSTANCE));
    }

    /** Returns a bucket's remote log manifest handle, if present. */
    public Optional<RemoteLogManifestHandle> getRemoteLogManifestHandle(TableBucket tableBucket) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.UPSERT_REMOTE_LOG_MANIFEST,
                                tableBucketBytes(tableBucket)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(
                JsonSerdeUtils.readValue(value, RemoteLogManifestHandleJsonSerde.INSTANCE));
    }

    /** Removes a bucket's remote log manifest handle. */
    public void removeRemoteLogManifestHandle(TableBucket tableBucket) throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.DELETE_REMOTE_LOG_MANIFEST,
                tableBucketBytes(tableBucket),
                EMPTY);
    }

    /**
     * Lists the remote-log-manifest handles of every bucket of {@code tableId} (or of {@code
     * partitionId} when non-null). Mirrors {@code ZooKeeperClient.listRemoteLogManifestHandles},
     * which enumerated the {@code .../buckets} children; here the buckets are recovered from the
     * record keys via a per-table(-partition) prefix scan.
     */
    public List<TableBucketAndManifest> listRemoteLogManifestHandles(
            long tableId, @Nullable Long partitionId) {
        requireLeader();
        byte[] prefix = remoteLogScanPrefix(tableId, partitionId);
        int bucketOffset =
                prefix.length; // the 4-byte bucket id is the id-less remainder of the key
        List<TableBucketAndManifest> result = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            int bucket = intFromBytes(entry[0], bucketOffset);
            TableBucket tableBucket =
                    partitionId == null
                            ? new TableBucket(tableId, bucket)
                            : new TableBucket(tableId, partitionId, bucket);
            RemoteLogManifestHandle handle =
                    JsonSerdeUtils.readValue(entry[1], RemoteLogManifestHandleJsonSerde.INSTANCE);
            result.add(new TableBucketAndManifest(tableBucket, handle));
        }
        return result;
    }

    /**
     * Prefix that bounds one table's (or one partition's) remote-log-manifest records, using the
     * same {@code tableId + flag [+ partitionId]} prefix of {@link #tableBucketBytes} (the trailing
     * 4-byte bucket id is what the scan enumerates). The flag byte keeps a non-partitioned table's
     * keys disjoint from any of its partitions' keys, so the scan never crosses that boundary.
     */
    private static byte[] remoteLogScanPrefix(long tableId, @Nullable Long partitionId) {
        byte[] header = MetadataKeys.prefixOf(MetadataRecord.Type.UPSERT_REMOTE_LOG_MANIFEST);
        ByteBuffer buffer = ByteBuffer.allocate(header.length + (partitionId == null ? 9 : 17));
        buffer.put(header);
        buffer.putLong(tableId);
        if (partitionId == null) {
            buffer.put((byte) 0);
        } else {
            buffer.put((byte) 1);
            buffer.putLong(partitionId);
        }
        return buffer.array();
    }

    // ---- lake table (former /metadata/databases/.../tables/.../lake_table) ----

    /** Upserts a table's lake-table metadata (keyed by table id). */
    public void upsertLakeTable(long tableId, LakeTable lakeTable) throws Exception {
        requireLeader();
        // UPSERT is a plain put; the ZK backend's isUpdate flag is a znode-lifecycle detail with no
        // Raft analog (the replicated record IS the lake-table state).
        append(
                MetadataRecord.Type.UPSERT_LAKE_TABLE,
                longToBytes(tableId),
                JsonSerdeUtils.writeValueAsBytes(lakeTable, LakeTableJsonSerde.INSTANCE));
    }

    /**
     * Upserts a table's lake-table metadata, ignoring {@code isUpdate} (a ZK create-vs-setData
     * lifecycle distinction with no Raft analog; the replicated record is the state). Delegates to
     * {@link #upsertLakeTable(long, LakeTable)}.
     */
    public void upsertLakeTable(long tableId, LakeTable lakeTable, boolean isUpdate)
            throws Exception {
        upsertLakeTable(tableId, lakeTable);
    }

    /** Returns a table's lake-table metadata, if present. */
    public Optional<LakeTable> getLakeTable(long tableId) {
        requireLeader();
        byte[] value =
                store.get(keyOf(MetadataRecord.Type.UPSERT_LAKE_TABLE, longToBytes(tableId)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, LakeTableJsonSerde.INSTANCE));
    }

    /**
     * Returns a table's lake-table snapshot (the latest when {@code snapshotId} is null), if the
     * table has lake-table metadata. Thin derivation over {@link #getLakeTable}, mirroring {@code
     * ZooKeeperClient.getLakeTableSnapshot}.
     */
    public Optional<LakeTableSnapshot> getLakeTableSnapshot(long tableId, @Nullable Long snapshotId)
            throws Exception {
        Optional<LakeTable> optLakeTable = getLakeTable(tableId);
        if (!optLakeTable.isPresent()) {
            return Optional.empty();
        }
        LakeTable lakeTable = optLakeTable.get();
        return Optional.ofNullable(
                snapshotId == null
                        ? lakeTable.getOrReadLatestTableSnapshot()
                        : lakeTable.getOrReadTableSnapshot(snapshotId));
    }

    /**
     * Returns a table's latest READABLE lake-table snapshot, if present. Thin derivation over
     * {@link #getLakeTable}, mirroring {@code ZooKeeperClient.getLatestReadableLakeTableSnapshot}.
     */
    public Optional<LakeTableSnapshot> getLatestReadableLakeTableSnapshot(long tableId)
            throws Exception {
        Optional<LakeTable> optLakeTable = getLakeTable(tableId);
        if (!optLakeTable.isPresent()) {
            return Optional.empty();
        }
        return Optional.ofNullable(optLakeTable.get().getOrReadLatestReadableTableSnapshot());
    }

    /**
     * Returns {@code partitionId -> PhysicalTablePath} for every partition of the given tables.
     * Mirrors {@code ZooKeeperClient.getPartitionIdAndPaths}, assembled here from the per-table
     * partition registrations ({@link #getPartitionRegistrations(TablePath)}) rather than ZK
     * children lookups.
     */
    public Map<Long, PhysicalTablePath> getPartitionIdAndPaths(Collection<TablePath> tablePaths) {
        requireLeader();
        Map<Long, PhysicalTablePath> result = new LinkedHashMap<>();
        for (TablePath tablePath : tablePaths) {
            for (Map.Entry<String, PartitionRegistration> entry :
                    getPartitionRegistrations(tablePath).entrySet()) {
                result.put(
                        entry.getValue().getPartitionId(),
                        PhysicalTablePath.of(tablePath, entry.getKey()));
            }
        }
        return result;
    }

    // ---- kv bucket snapshot (former .../buckets/[id]/snapshots/[snapshotId]) ----

    /** Registers a bucket snapshot, keyed by {@code (tableBucket, snapshotId)}. */
    public void registerTableBucketSnapshot(TableBucket tableBucket, BucketSnapshot snapshot)
            throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.REGISTER_BUCKET_SNAPSHOT,
                bucketSnapshotKeyBody(tableBucket, snapshot.getSnapshotId()),
                JsonSerdeUtils.writeValueAsBytes(snapshot, BucketSnapshotJsonSerde.INSTANCE));
    }

    /** Deletes the bucket snapshot {@code (tableBucket, snapshotId)}. */
    public void deleteTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        requireLeader();
        append(
                MetadataRecord.Type.DELETE_BUCKET_SNAPSHOT,
                bucketSnapshotKeyBody(tableBucket, snapshotId),
                EMPTY);
    }

    /** Returns the bucket snapshot {@code (tableBucket, snapshotId)}, if present. */
    public Optional<BucketSnapshot> getTableBucketSnapshot(
            TableBucket tableBucket, long snapshotId) {
        requireLeader();
        byte[] value =
                store.get(
                        keyOf(
                                MetadataRecord.Type.REGISTER_BUCKET_SNAPSHOT,
                                bucketSnapshotKeyBody(tableBucket, snapshotId)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JsonSerdeUtils.readValue(value, BucketSnapshotJsonSerde.INSTANCE));
    }

    /** Returns the highest snapshot id registered for {@code tableBucket}, if any. */
    public OptionalLong getTableBucketLatestSnapshotId(TableBucket tableBucket) {
        requireLeader();
        byte[] prefix =
                keyOf(MetadataRecord.Type.REGISTER_BUCKET_SNAPSHOT, tableBucketBytes(tableBucket));
        boolean found = false;
        long max = Long.MIN_VALUE;
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            long id = longFromBytes(entry[0], prefix.length);
            if (!found || id > max) {
                max = id;
                found = true;
            }
        }
        return found ? OptionalLong.of(max) : OptionalLong.empty();
    }

    /** Returns the latest snapshot of {@code tableBucket}, if any. */
    public Optional<BucketSnapshot> getTableBucketLatestSnapshot(TableBucket tableBucket) {
        requireLeader();
        OptionalLong latest = getTableBucketLatestSnapshotId(tableBucket);
        return latest.isPresent()
                ? getTableBucketSnapshot(tableBucket, latest.getAsLong())
                : Optional.empty();
    }

    /**
     * Returns every {@code (snapshot, snapshotId)} registered for {@code tableBucket} (a prefix
     * scan over {@code REGISTER_BUCKET_SNAPSHOT} recovering the 8-byte snapshot id from each key).
     * Mirrors {@code ZooKeeperClient.getTableBucketAllSnapshotAndIds}.
     */
    public List<Tuple2<BucketSnapshot, Long>> getTableBucketAllSnapshotAndIds(
            TableBucket tableBucket) {
        requireLeader();
        byte[] prefix =
                keyOf(MetadataRecord.Type.REGISTER_BUCKET_SNAPSHOT, tableBucketBytes(tableBucket));
        List<Tuple2<BucketSnapshot, Long>> result = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            long snapshotId = longFromBytes(entry[0], prefix.length);
            BucketSnapshot snapshot =
                    JsonSerdeUtils.readValue(entry[1], BucketSnapshotJsonSerde.INSTANCE);
            result.add(Tuple2.of(snapshot, snapshotId));
        }
        return result;
    }

    /**
     * Lists the ids of all snapshots registered for {@code tableBucket}, ascending (the Raft
     * equivalent of {@code getChildren(BucketSnapshotsZNode.path(tableBucket))}).
     */
    public List<Long> listBucketSnapshotIds(TableBucket tableBucket) {
        requireLeader();
        byte[] prefix =
                keyOf(MetadataRecord.Type.REGISTER_BUCKET_SNAPSHOT, tableBucketBytes(tableBucket));
        List<Long> ids = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            ids.add(longFromBytes(entry[0], prefix.length));
        }
        Collections.sort(ids);
        return ids;
    }

    /**
     * Returns the latest snapshot per bucket of {@code tableId} (empty {@link Optional} for buckets
     * with no snapshot), seeded from the table assignment. Empty map if the table is unassigned.
     */
    public Map<Integer, Optional<BucketSnapshot>> getTableLatestBucketSnapshot(long tableId) {
        requireLeader();
        Optional<TableAssignment> assignment = getTableAssignment(tableId);
        return assignment.isPresent()
                ? bucketSnapshots(tableId, null, assignment.get())
                : new LinkedHashMap<>();
    }

    /** Partition variant of {@link #getTableLatestBucketSnapshot}. */
    public Map<Integer, Optional<BucketSnapshot>> getPartitionLatestBucketSnapshot(
            long partitionId) {
        requireLeader();
        Optional<PartitionAssignment> assignment = getPartitionAssignment(partitionId);
        return assignment.isPresent()
                ? bucketSnapshots(assignment.get().getTableId(), partitionId, assignment.get())
                : new LinkedHashMap<>();
    }

    /** Builds the per-bucket latest-snapshot map from an assignment's bucket set. */
    private Map<Integer, Optional<BucketSnapshot>> bucketSnapshots(
            long tableId, @Nullable Long partitionId, TableAssignment assignment) {
        Map<Integer, Optional<BucketSnapshot>> result = new LinkedHashMap<>();
        for (Integer bucket : assignment.getBuckets()) {
            result.put(
                    bucket,
                    getTableBucketLatestSnapshot(new TableBucket(tableId, partitionId, bucket)));
        }
        return result;
    }

    // ---- sequence id counters ----

    @Override
    public long getTableIdAndIncrement() throws Exception {
        requireLeader();
        return tableIdCounter.getAndIncrement();
    }

    @Override
    public long getPartitionIdAndIncrement() throws Exception {
        requireLeader();
        return partitionIdCounter.getAndIncrement();
    }

    @Override
    public long getWriterIdAndIncrement() throws Exception {
        requireLeader();
        return writerIdCounter.getAndIncrement();
    }

    /**
     * Reserves a contiguous block of {@code count} writer ids from the single owned "writer"
     * counter and returns the first id of the range (the caller owns {@code [start, start +
     * count)}). Used by the leader-side {@link AllocateIdRpcServer} to serve tablet-server
     * writer-id allocation over the {@code AllocateId} RPC. Routes to the owned counter (not a
     * duplicate one) so the persisted high-water mark has a single writer.
     */
    long allocateWriterIdBlock(long count) throws Exception {
        requireLeader();
        return writerIdCounter.getAndAdd(count);
    }

    // ---- leader election ----

    @Override
    public LeaderElection newLeaderSelector(String serverId) {
        // The RaftLeaderSelector is built alongside this client and bound to this node's raft
        // server.
        return leaderSelector;
    }

    @Override
    public void close() {
        // A read-only follower view (followerReadView) owns no writer/leaderSelector/server — its
        // store is fed and owned externally — so there is nothing to close.
        if (followerReadOnly) {
            return;
        }
        try {
            writer.close();
        } catch (Exception e) {
            LOG.warn("Failed to close RaftJournalWriter", e);
        }
        leaderSelector.close();
        if (ownedServer != null) {
            try {
                ownedServer.close();
            } catch (Exception e) {
                LOG.warn("Failed to close RaftMetadataServer", e);
            }
        }
        try {
            store.close();
        } catch (Exception e) {
            LOG.warn("Failed to close RaftMetadataStore / MetadataKvStore", e);
        }
    }

    // ---- internals ----

    /** The underlying journal writer, for wiring leader-side components (e.g. liveness fencing). */
    @VisibleForTesting
    RaftJournalWriter writerForTesting() {
        return writer;
    }

    // ---- leader-side services accessors (coordinator integration, §5.4.2 / §5.8) ----
    // These are only meaningful on a client that bootstrapped its own server via create(...); a
    // dependency-injected client (tests of the store surface) has no owned server/state machine.

    /** The journal writer, for wiring leader-side components (liveness fencing, id allocation). */
    RaftJournalWriter journalWriter() {
        return writer;
    }

    /**
     * The local metadata store (leader = linearizable), for wiring leader-side components that
     * build their own {@link RaftSequenceIDCounter} instances (e.g. the {@link
     * AllocateIdRpcServer}'s per-key auto-increment-column / bucket-snapshot counters).
     */
    RaftMetadataStore store() {
        return store;
    }

    /** The leader selector, for {@code isLeader}/epoch of the leader-side services. */
    RaftLeaderSelector leaderSelector() {
        return leaderSelector;
    }

    /**
     * The in-process watch service (fired from the apply loop), for wiring the leader-side change
     * watchers ({@link RaftCoordinatorChangeWatchers}) that replace the ZooKeeper CuratorCache
     * watchers on the Raft backend.
     */
    MetadataWatchService watchService() {
        return store.watchService();
    }

    /** The owned embedded Raft server (RaftLog access for the metadata stream). */
    RaftMetadataServer ownedServer() {
        return requireOwned(ownedServer, "owned Raft server");
    }

    /**
     * Fills a {@link GetCoordinatorRaftStatusResponse} for the §7.4.5 operator status RPC. Safe to
     * call on leader or follower; voter lag details are best-effort when the owned Raft server is
     * available.
     */
    public void populateCoordinatorRaftStatus(
            GetCoordinatorRaftStatusResponse response, LeaderElection election) {
        response.setIsRaftMode(true);
        response.setLeaderEpoch(election.currentEpoch().coordinatorEpoch());
        try {
            Optional<CoordinatorAddress> leader = getCoordinatorLeaderAddress();
            if (leader.isPresent()) {
                List<Endpoint> endpoints = leader.get().getEndpoints();
                if (!endpoints.isEmpty()) {
                    Endpoint ep = endpoints.get(0);
                    response.setLeader(
                            new PbServerNode()
                                    .setNodeId(0)
                                    .setHost(ep.getHost())
                                    .setPort(ep.getPort()));
                }
            }
        } catch (Exception e) {
            LOG.debug("Could not resolve coordinator leader address for raft status", e);
        }
        if (ownedServer != null && ownedStateMachine != null) {
            try {
                org.apache.fluss.shaded.ratis.org.apache.ratis.server.RaftServer.Division division =
                        ownedServer.getRaftServer().getDivision(RaftMetadataServer.RAFT_GROUP_ID);
                response.setCommitIndex(division.getRaftLog().getLastCommittedIndex());
                response.setAppliedIndex(ownedStateMachine.getLastAppliedTermIndex().getIndex());
                if (!election.isLeader()) {
                    // Follower: identity fields are enough; hint CLI to retry on the leader for
                    // the full voter table.
                    if (response.hasLeader()) {
                        response.setRedirect(response.getLeader());
                    }
                } else {
                    response.addAllVoters(listVoters());
                }
            } catch (Exception e) {
                LOG.debug("Could not read Ratis division info for raft status", e);
            }
        }
    }

    /**
     * The owned state machine (snapshot capture + apply-listener wiring for the metadata stream).
     */
    FlussMetadataStateMachine ownedStateMachine() {
        return requireOwned(ownedStateMachine, "owned state machine");
    }

    private static <T> T requireOwned(@Nullable T value, String what) {
        if (value == null) {
            throw new IllegalStateException(
                    "This RaftMetadataClient did not bootstrap its own Raft stack, so its "
                            + what
                            + " is unavailable; use RaftMetadataClient.create(...) for the "
                            + "coordinator integration.");
        }
        return value;
    }

    private void requireLeader() {
        if (followerReadOnly) {
            // A follower read view serves reads from its replica store without a leader guard.
            return;
        }
        if (!leaderSelector.isLeader()) {
            throw new NotCoordinatorLeaderException(
                    "This coordinator is not the raft leader; retry against the leader.");
        }
    }

    /**
     * Appends a pre-built {@link MetadataRecord} (used by the ZK→Raft migration tool). Resolves
     * after commit + local apply.
     */
    public void appendRaw(MetadataRecord record) throws Exception {
        requireLeader();
        writer.append(record).get();
    }

    /** Whether this node currently holds Raft leadership. */
    public boolean isLeader() {
        return leaderSelector != null && leaderSelector.isLeader();
    }

    /**
     * Per-segment append latency attribution (§8.9.1): how much of a metadata write is the Ratis
     * round-trip versus the commit-to-apply handoff. Diagnostics only.
     */
    public AppendLatencyStats appendLatencyStats() {
        return writer.latencyStats();
    }

    private void append(MetadataRecord.Type type, byte[] key, byte[] payload) throws Exception {
        // append resolves after commit+local-apply; NO separate store.apply (single-writer,
        // §5.1.4).
        writer.append(new MetadataRecord(type, key, payload)).get();
    }

    /** Single-record variant of {@link #append} for callers that build the record themselves. */
    private void appendRecord(MetadataRecord record) throws Exception {
        writer.append(record).get();
    }

    /**
     * Sends {@code records} as independent log entries and waits once for apply. {@link
     * #requireLeader()} is taken once for the batch. If any entry fails, a prefix of the batch may
     * already be committed (see {@link RaftJournalWriter#appendAll}).
     */
    private void appendAll(List<MetadataRecord> records) throws Exception {
        requireLeader();
        try {
            writer.appendAll(records).get();
        } catch (Exception e) {
            LOG.error(
                    "Batched metadata append of {} independent log entries failed; "
                            + "a prefix of the batch may already be committed and applied.",
                    records.size(),
                    e);
            throw e;
        }
    }

    private static byte[] coordinatorLeaderKey() {
        return keyOf(MetadataRecord.Type.REGISTER_COORDINATOR_LEADER, EMPTY);
    }

    /**
     * Computes the store key for {@code type} + {@code logicalKey} (payload is irrelevant here).
     */
    private static byte[] keyOf(MetadataRecord.Type type, byte[] logicalKey) {
        return MetadataKeys.keyOf(new MetadataRecord(type, logicalKey, EMPTY));
    }

    private static byte[] tablePathBytes(TablePath tablePath) {
        return tablePath.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] dbNameBytes(String database) {
        return database.getBytes(StandardCharsets.UTF_8);
    }

    /** Schema record key body: {@code tablePath + 0x00 + 4-byte BE schemaId}. */
    private static byte[] schemaKeyBytes(TablePath tablePath, int schemaId) {
        byte[] tp = tablePathBytes(tablePath);
        return ByteBuffer.allocate(tp.length + 1 + 4)
                .put(tp)
                .put(KEY_DELIM)
                .putInt(schemaId)
                .array();
    }

    /** Per-table schema scan body: {@code tablePath + 0x00} (bounds the path, excludes the id). */
    private static byte[] schemaTableScanBody(TablePath tablePath) {
        byte[] tp = tablePathBytes(tablePath);
        return ByteBuffer.allocate(tp.length + 1).put(tp).put(KEY_DELIM).array();
    }

    /** Partition record key body: {@code tablePath + 0x00 + partitionName}. */
    private static byte[] partitionKeyBytes(TablePath tablePath, String partitionName) {
        byte[] tp = tablePathBytes(tablePath);
        byte[] name = partitionName.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(tp.length + 1 + name.length)
                .put(tp)
                .put(KEY_DELIM)
                .put(name)
                .array();
    }

    /** Per-table partition scan body: {@code tablePath + 0x00} (the remainder is the name). */
    private static byte[] partitionTableScanBody(TablePath tablePath) {
        byte[] tp = tablePathBytes(tablePath);
        return ByteBuffer.allocate(tp.length + 1).put(tp).put(KEY_DELIM).array();
    }

    private static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    /**
     * Recovers a UTF-8 logical-key body that follows the fixed {@code offset}-byte store header.
     */
    private static String stringFromKey(byte[] key, int offset) {
        return new String(key, offset, key.length - offset, StandardCharsets.UTF_8);
    }

    /** Canonical byte key for a bucket: tableId, optional partitionId, bucket. */
    private static byte[] tableBucketBytes(TableBucket tableBucket) {
        Long partitionId = tableBucket.getPartitionId();
        ByteBuffer buffer = ByteBuffer.allocate(partitionId == null ? 13 : 21);
        buffer.putLong(tableBucket.getTableId());
        if (partitionId == null) {
            buffer.put((byte) 0);
        } else {
            buffer.put((byte) 1);
            buffer.putLong(partitionId);
        }
        buffer.putInt(tableBucket.getBucket());
        return buffer.array();
    }

    private static byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    private static int intFromBytes(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, 4).getInt();
    }

    private static long longFromBytes(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, 8).getLong();
    }

    /** Bucket-snapshot key body: {@code tableBucket + 8-byte BE snapshotId}. */
    private static byte[] bucketSnapshotKeyBody(TableBucket tableBucket, long snapshotId) {
        byte[] tb = tableBucketBytes(tableBucket);
        return ByteBuffer.allocate(tb.length + 8).put(tb).putLong(snapshotId).array();
    }
}
