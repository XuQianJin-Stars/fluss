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

package org.apache.fluss.server.tools;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.GlobalConfiguration;
import org.apache.fluss.metrics.registry.MetricRegistryImpl;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.AddRaftVoterRequest;
import org.apache.fluss.rpc.messages.AddRaftVoterResponse;
import org.apache.fluss.rpc.messages.GetCoordinatorRaftStatusRequest;
import org.apache.fluss.rpc.messages.GetCoordinatorRaftStatusResponse;
import org.apache.fluss.rpc.messages.PbRaftVoterState;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.RemoveRaftVoterRequest;
import org.apache.fluss.rpc.messages.RemoveRaftVoterResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.server.coordinator.raft.RaftUnsafeRecover;
import org.apache.fluss.server.tools.migration.ZkToRaftMigrationTool;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Umbrella ops CLI invoked by {@code bin/fluss}. Supports Raft lifecycle/status commands and the
 * ZooKeeper→Raft migration helpers (see zookeeper-removal-design.md §7.4).
 */
public final class FlussCli {

    private static final long DEFAULT_TIMEOUT_MS = 10_000L;

    private FlussCli() {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        if (args.length == 0) {
            printUsage();
            return 1;
        }
        try {
            if ("raft".equals(args[0])) {
                return runRaft(shift(args));
            }
            if ("leader".equals(args[0])) {
                return runRaftStatus(shift(args), true);
            }
            if ("zk-to-raft-export".equals(args[0])) {
                return ZkToRaftMigrationTool.exportMain(shift(args));
            }
            if ("raft-import".equals(args[0]) || "zk-to-raft-import".equals(args[0])) {
                return ZkToRaftMigrationTool.importMain(shift(args));
            }
            if ("zk-to-raft-validate".equals(args[0])) {
                return ZkToRaftMigrationTool.validateMain(shift(args));
            }
            if ("zk-to-raft-live-migrate".equals(args[0])) {
                return ZkToRaftMigrationTool.liveMigrateMain(shift(args));
            }
            System.err.println("Unknown command: " + args[0]);
            printUsage();
            return 1;
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace(System.err);
            return 1;
        }
    }

    private static int runRaft(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println(
                    "Usage: fluss raft <bootstrap|status|leader|add-voter|remove-voter|unsafe-recover> [options]");
            return 1;
        }
        if ("bootstrap".equals(args[0])) {
            return runRaftBootstrap(shift(args));
        }
        if ("status".equals(args[0])) {
            return runRaftStatus(shift(args), false);
        }
        if ("leader".equals(args[0])) {
            return runRaftStatus(shift(args), true);
        }
        if ("add-voter".equals(args[0])) {
            return runRaftAddVoter(shift(args));
        }
        if ("remove-voter".equals(args[0])) {
            return runRaftRemoveVoter(shift(args));
        }
        if ("unsafe-recover".equals(args[0])) {
            return runRaftUnsafeRecover(shift(args));
        }
        System.err.println("Unknown raft subcommand: " + args[0]);
        return 1;
    }

    /**
     * Formats {@code coordinator.raft.dir} and records the initial voter set so the first
     * coordinator start can join a fresh quorum (analogous to KRaft storage format).
     */
    private static int runRaftBootstrap(String[] args) throws Exception {
        String configDir = null;
        for (int i = 0; i < args.length; i++) {
            if (("-c".equals(args[i]) || "--config".equals(args[i])) && i + 1 < args.length) {
                configDir = args[++i];
            }
        }
        if (configDir == null) {
            configDir = System.getenv("FLUSS_CONF_DIR");
        }
        if (configDir == null || configDir.trim().isEmpty()) {
            System.err.println("ERROR: --config <dir> or FLUSS_CONF_DIR is required for bootstrap");
            return 1;
        }
        Configuration conf = GlobalConfiguration.loadConfiguration(configDir, null);
        String raftDir = conf.getOptional(ConfigOptions.COORDINATOR_RAFT_DIR).orElse(null);
        if (raftDir == null || raftDir.trim().isEmpty()) {
            System.err.println("ERROR: coordinator.raft.dir must be set in " + configDir);
            return 1;
        }
        String voters = conf.getOptional(ConfigOptions.COORDINATOR_RAFT_VOTERS).orElse(null);
        if (voters == null || voters.trim().isEmpty()) {
            System.err.println("ERROR: coordinator.raft.voters must be set for bootstrap");
            return 1;
        }
        File dir = new File(raftDir);
        if (!dir.exists() && !dir.mkdirs()) {
            System.err.println("ERROR: cannot create raft dir " + raftDir);
            return 1;
        }
        File marker = new File(dir, "fluss-raft-bootstrap.marker");
        if (marker.exists()) {
            System.out.println("Raft dir already bootstrapped: " + raftDir);
            return 0;
        }
        String content =
                "voters="
                        + voters.trim()
                        + "\n"
                        + "bootstrapped_at_ms="
                        + System.currentTimeMillis()
                        + "\n";
        Files.write(marker.toPath(), content.getBytes(StandardCharsets.UTF_8));
        System.out.println("Bootstrapped Raft dir at " + raftDir + " with voters=" + voters);
        return 0;
    }

    private static int runRaftStatus(String[] args, boolean leaderOnly) throws Exception {
        CoordinatorEndpoint endpoint = parseCoordinatorEndpoint(args);
        Configuration conf = new Configuration();
        MetricRegistryImpl registry = new MetricRegistryImpl(Collections.emptyList());
        ClientMetricGroup metricGroup = new ClientMetricGroup(registry, "fluss-cli");
        try (RpcClient rpcClient = RpcClient.create(conf, metricGroup)) {
            if (endpoint.watch && !leaderOnly) {
                return RaftStatusWatchLoop.run(
                        endpoint.intervalMs,
                        () -> Thread.currentThread().isInterrupted(),
                        interval -> Thread.sleep(interval),
                        () -> {
                            try {
                                return fetchAndPrintStatus(rpcClient, endpoint, false);
                            } catch (Exception e) {
                                System.err.println("ERROR: " + e.getMessage());
                                return 1;
                            }
                        });
            }
            return fetchAndPrintStatus(rpcClient, endpoint, leaderOnly);
        }
    }

    private static int fetchAndPrintStatus(
            RpcClient rpcClient, CoordinatorEndpoint endpoint, boolean leaderOnly)
            throws Exception {
        AdminReadOnlyGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> endpoint.node, rpcClient, AdminReadOnlyGateway.class);
        GetCoordinatorRaftStatusResponse resp =
                gateway.getCoordinatorRaftStatus(new GetCoordinatorRaftStatusRequest())
                        .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // One transparent retry against redirect for the leader-only voter table.
        if (resp.hasRedirect() && !leaderOnly) {
            PbServerNode redirect = resp.getRedirect();
            ServerNode leaderNode =
                    new ServerNode(
                            redirect.getNodeId(),
                            redirect.getHost(),
                            redirect.getPort(),
                            ServerType.COORDINATOR);
            AdminReadOnlyGateway leaderGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> leaderNode, rpcClient, AdminReadOnlyGateway.class);
            resp =
                    leaderGateway
                            .getCoordinatorRaftStatus(new GetCoordinatorRaftStatusRequest())
                            .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }

        if (leaderOnly) {
            if (!resp.hasLeader()) {
                System.out.println("no leader");
                return 1;
            }
            PbServerNode leader = resp.getLeader();
            System.out.println(leader.getHost() + ":" + leader.getPort());
            return 0;
        }

        if (endpoint.json) {
            printJson(resp);
        } else {
            printHuman(resp);
        }
        return resp.hasLeader() || !resp.isIsRaftMode() ? 0 : 1;
    }

    /**
     * {@code fluss raft add-voter <host:raftPort> [--coordinator host:port]} — leader-served
     * single-server membership change (§5.10).
     */
    private static int runRaftAddVoter(String[] args) throws Exception {
        if (args.length == 0 || args[0].startsWith("-")) {
            System.err.println(
                    "Usage: fluss raft add-voter <host:raftPort> [--coordinator host:port]");
            return 1;
        }
        String address = args[0];
        CoordinatorEndpoint endpoint = parseCoordinatorEndpoint(shift(args));
        return withAdminGateway(
                endpoint,
                gateway -> {
                    AddRaftVoterResponse resp =
                            gateway.addRaftVoter(new AddRaftVoterRequest().setAddress(address))
                                    .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    printVoters(resp.getVotersList());
                    return 0;
                });
    }

    /**
     * {@code fluss raft remove-voter <serverId> [--coordinator host:port]} — refuses removals that
     * would break quorum (§5.10). {@code serverId} is the peer id ({@code host_port}) or address
     * ({@code host:port}).
     */
    private static int runRaftRemoveVoter(String[] args) throws Exception {
        if (args.length == 0 || args[0].startsWith("-")) {
            System.err.println(
                    "Usage: fluss raft remove-voter <serverId> [--coordinator host:port]");
            return 1;
        }
        String serverId = args[0];
        CoordinatorEndpoint endpoint = parseCoordinatorEndpoint(shift(args));
        return withAdminGateway(
                endpoint,
                gateway -> {
                    RemoveRaftVoterResponse resp =
                            gateway.removeRaftVoter(
                                            new RemoveRaftVoterRequest().setServerId(serverId))
                                    .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    printVoters(resp.getVotersList());
                    return 0;
                });
    }

    /**
     * {@code fluss raft unsafe-recover --i-understand-data-loss [--config <conf-dir>]} — offline,
     * guarded quorum-loss recovery (§9.4). Forces the local Raft group to {@code {survivor}} by
     * rewriting {@code raft-meta.conf}. Never automatic.
     */
    private static int runRaftUnsafeRecover(String[] args) throws Exception {
        boolean acknowledged = false;
        String configDir = null;
        for (int i = 0; i < args.length; i++) {
            if ("--i-understand-data-loss".equals(args[i])) {
                acknowledged = true;
            } else if (("-c".equals(args[i]) || "--config".equals(args[i]))
                    && i + 1 < args.length) {
                configDir = args[++i];
            }
        }
        if (!acknowledged) {
            System.err.println("ERROR: refusing unsafe-recover without --i-understand-data-loss");
            System.err.println(
                    "Usage: fluss raft unsafe-recover --i-understand-data-loss [--config <conf-dir>]");
            System.err.println(
                    "This offline tool forces the Raft group under coordinator.raft.dir to a");
            System.err.println(
                    "single-node {survivor}. Unreplicated commits on lost voters may be discarded.");
            System.err.println("Stop the coordinator on this node before running.");
            return 1;
        }
        if (configDir == null) {
            configDir = System.getenv("FLUSS_CONF_DIR");
        }
        if (configDir == null || configDir.trim().isEmpty()) {
            System.err.println(
                    "ERROR: --config <dir> or FLUSS_CONF_DIR is required for unsafe-recover");
            return 1;
        }
        Configuration conf = GlobalConfiguration.loadConfiguration(configDir, null);
        String summary = RaftUnsafeRecover.recover(conf);
        System.out.println("WARNING: unsafe Raft quorum recovery completed.");
        System.out.println(summary);
        System.out.println(
                "Next: set coordinator.raft.voters to the survivor only, start this coordinator,");
        System.out.println("then fluss raft add-voter for replacement peers.");
        return 0;
    }

    private static int withAdminGateway(CoordinatorEndpoint endpoint, AdminGatewayAction action)
            throws Exception {
        Configuration conf = new Configuration();
        MetricRegistryImpl registry = new MetricRegistryImpl(Collections.emptyList());
        ClientMetricGroup metricGroup = new ClientMetricGroup(registry, "fluss-cli");
        try (RpcClient rpcClient = RpcClient.create(conf, metricGroup)) {
            // Prefer the current Raft leader when known so membership RPCs succeed without a
            // manual retry.
            ServerNode target = endpoint.node;
            AdminReadOnlyGateway readGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> endpoint.node, rpcClient, AdminReadOnlyGateway.class);
            try {
                GetCoordinatorRaftStatusResponse status =
                        readGateway
                                .getCoordinatorRaftStatus(new GetCoordinatorRaftStatusRequest())
                                .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (status.hasLeader()) {
                    PbServerNode leader = status.getLeader();
                    target =
                            new ServerNode(
                                    leader.getNodeId(),
                                    leader.getHost(),
                                    leader.getPort(),
                                    ServerType.COORDINATOR);
                } else if (status.hasRedirect()) {
                    PbServerNode redirect = status.getRedirect();
                    target =
                            new ServerNode(
                                    redirect.getNodeId(),
                                    redirect.getHost(),
                                    redirect.getPort(),
                                    ServerType.COORDINATOR);
                }
            } catch (Exception e) {
                // Fall through and try the configured coordinator endpoint.
            }
            ServerNode finalTarget = target;
            AdminGateway gateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> finalTarget, rpcClient, AdminGateway.class);
            return action.run(gateway);
        }
    }

    static CoordinatorEndpoint parseCoordinatorEndpoint(String[] args) {
        String host = "127.0.0.1";
        int port = 9123;
        boolean json = false;
        boolean watch = false;
        long intervalMs = RaftStatusWatchLoop.DEFAULT_INTERVAL_MS;
        for (int i = 0; i < args.length; i++) {
            if (("--coordinator".equals(args[i]) || "-c".equals(args[i])) && i + 1 < args.length) {
                String[] hp = args[++i].split(":");
                host = hp[0];
                if (hp.length > 1) {
                    port = Integer.parseInt(hp[1]);
                }
            } else if ("--json".equals(args[i])) {
                json = true;
            } else if ("--watch".equals(args[i])) {
                watch = true;
            } else if ("--interval".equals(args[i]) && i + 1 < args.length) {
                intervalMs = Long.parseLong(args[++i]);
                watch = true;
            }
        }
        return new CoordinatorEndpoint(
                new ServerNode(0, host, port, ServerType.COORDINATOR), json, watch, intervalMs);
    }

    private static void printVoters(List<PbRaftVoterState> voters) {
        System.out.println("Voters (" + voters.size() + "):");
        for (PbRaftVoterState voter : voters) {
            System.out.println(
                    "  id="
                            + voter.getServerId()
                            + "  "
                            + voter.getAddress()
                            + "  "
                            + voter.getRole()
                            + "  matchIndex="
                            + voter.getMatchIndex());
        }
    }

    private static void printHuman(GetCoordinatorRaftStatusResponse resp) {
        System.out.println("Raft mode:     " + resp.isIsRaftMode());
        if (resp.hasLeader()) {
            PbServerNode leader = resp.getLeader();
            System.out.println(
                    "Leader:        "
                            + leader.getHost()
                            + ":"
                            + leader.getPort()
                            + "  (server id "
                            + leader.getNodeId()
                            + ")");
        } else {
            System.out.println("Leader:        (none)");
        }
        if (resp.hasLeaderEpoch()) {
            System.out.println("Epoch (term):  " + resp.getLeaderEpoch());
        }
        if (resp.hasCommitIndex() || resp.hasAppliedIndex()) {
            System.out.println(
                    "Commit index:  "
                            + (resp.hasCommitIndex() ? resp.getCommitIndex() : "-")
                            + "    Applied index: "
                            + (resp.hasAppliedIndex() ? resp.getAppliedIndex() : "-"));
        }
        if (resp.getVotersCount() > 0) {
            System.out.println("Voters:");
            long commit = resp.hasCommitIndex() ? resp.getCommitIndex() : 0L;
            for (PbRaftVoterState voter : resp.getVotersList()) {
                long lag = commit - voter.getMatchIndex();
                System.out.println(
                        "  id="
                                + voter.getServerId()
                                + "  "
                                + voter.getAddress()
                                + "  "
                                + voter.getRole()
                                + "  matchIndex="
                                + voter.getMatchIndex()
                                + "  lag="
                                + lag);
            }
        }
    }

    private static void printJson(GetCoordinatorRaftStatusResponse resp) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"is_raft_mode\":").append(resp.isIsRaftMode());
        if (resp.hasLeader()) {
            PbServerNode leader = resp.getLeader();
            sb.append(",\"leader\":\"")
                    .append(leader.getHost())
                    .append(':')
                    .append(leader.getPort())
                    .append('"');
            sb.append(",\"leader_id\":").append(leader.getNodeId());
        }
        if (resp.hasLeaderEpoch()) {
            sb.append(",\"leader_epoch\":").append(resp.getLeaderEpoch());
        }
        if (resp.hasCommitIndex()) {
            sb.append(",\"commit_index\":").append(resp.getCommitIndex());
        }
        if (resp.hasAppliedIndex()) {
            sb.append(",\"applied_index\":").append(resp.getAppliedIndex());
        }
        sb.append('}');
        System.out.println(sb.toString());
    }

    private static void printUsage() {
        System.err.println("Usage: fluss <command> [options]");
        System.err.println("Commands:");
        System.err.println("  raft bootstrap [--config <conf-dir>]");
        System.err.println(
                "  raft status [--coordinator host:port] [--json] [--watch] [--interval ms]");
        System.err.println("  raft leader  [--coordinator host:port]");
        System.err.println("  raft add-voter <host:raftPort> [--coordinator host:port]");
        System.err.println("  raft remove-voter <serverId> [--coordinator host:port]");
        System.err.println("  raft unsafe-recover --i-understand-data-loss [--config <conf-dir>]");
        System.err.println("  leader       [--coordinator host:port]");
        System.err.println("  zk-to-raft-export --zk-address <addr> --output <file>");
        System.err.println("  raft-import --input <file> [--raft-seeds host:port]");
        System.err.println("  zk-to-raft-validate --zk-address <addr> --input <file>");
        System.err.println(
                "  zk-to-raft-live-migrate --zk-address <addr> [--raft-seeds host:port] [--max-rounds N]");
    }

    private static String[] shift(String[] args) {
        if (args.length <= 1) {
            return new String[0];
        }
        String[] rest = new String[args.length - 1];
        System.arraycopy(args, 1, rest, 0, rest.length);
        return rest;
    }

    @FunctionalInterface
    private interface AdminGatewayAction {
        int run(AdminGateway gateway) throws Exception;
    }

    static final class CoordinatorEndpoint {
        final ServerNode node;
        final boolean json;
        final boolean watch;
        final long intervalMs;

        CoordinatorEndpoint(ServerNode node, boolean json, boolean watch, long intervalMs) {
            this.node = node;
            this.json = json;
            this.watch = watch;
            this.intervalMs = intervalMs;
        }
    }
}
