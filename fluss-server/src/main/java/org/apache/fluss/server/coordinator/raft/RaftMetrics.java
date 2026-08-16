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
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.DivisionInfo;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.RaftServer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.protocol.TermIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers coordinator Raft gauges from the live Ratis division / state machine (design §5.12).
 *
 * <p>Role encoding: {@code 0=follower}, {@code 1=candidate}, {@code 2=leader}, {@code -1=unknown}.
 */
@Internal
public final class RaftMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(RaftMetrics.class);

    /** Gauge value when this node is a Raft follower. */
    public static final long ROLE_FOLLOWER = 0L;
    /** Gauge value when this node is a Raft candidate. */
    public static final long ROLE_CANDIDATE = 1L;
    /** Gauge value when this node is a Raft leader. */
    public static final long ROLE_LEADER = 2L;
    /** Gauge value when the division is unavailable. */
    public static final long ROLE_UNKNOWN = -1L;

    private RaftMetrics() {}

    /**
     * Registers §5.12 Raft gauges on {@code metricGroup}, sourcing values from the client's owned
     * embedded Raft server.
     */
    public static void register(MetricGroup metricGroup, RaftMetadataClient client) {
        metricGroup.gauge(MetricNames.RAFT_ROLE, () -> role(client));
        metricGroup.gauge(MetricNames.RAFT_TERM, () -> term(client));
        metricGroup.gauge(MetricNames.RAFT_COMMIT_INDEX, () -> commitIndex(client));
        metricGroup.gauge(MetricNames.RAFT_APPLIED_INDEX, () -> appliedIndex(client));
        metricGroup.gauge(MetricNames.RAFT_APPLY_LAG, () -> applyLag(client));
        metricGroup.gauge(MetricNames.RAFT_FOLLOWER_LAG, () -> maxFollowerLag(client));
    }

    static long role(RaftMetadataClient client) {
        DivisionInfo info = infoOrNull(client);
        if (info == null) {
            return ROLE_UNKNOWN;
        }
        if (info.isLeader()) {
            return ROLE_LEADER;
        }
        if (info.isCandidate()) {
            return ROLE_CANDIDATE;
        }
        return ROLE_FOLLOWER;
    }

    static long term(RaftMetadataClient client) {
        DivisionInfo info = infoOrNull(client);
        return info == null ? -1L : info.getCurrentTerm();
    }

    static long commitIndex(RaftMetadataClient client) {
        try {
            RaftServer.Division division = divisionOrNull(client);
            return division == null ? -1L : division.getRaftLog().getLastCommittedIndex();
        } catch (Exception e) {
            LOG.debug("Could not read raft commit index", e);
            return -1L;
        }
    }

    static long appliedIndex(RaftMetadataClient client) {
        try {
            FlussMetadataStateMachine sm = client.ownedStateMachine();
            TermIndex ti = sm.getLastAppliedTermIndex();
            return ti == null ? -1L : ti.getIndex();
        } catch (Exception e) {
            LOG.debug("Could not read raft applied index", e);
            return -1L;
        }
    }

    static long applyLag(RaftMetadataClient client) {
        long commit = commitIndex(client);
        long applied = appliedIndex(client);
        if (commit < 0 || applied < 0) {
            return -1L;
        }
        return Math.max(0L, commit - applied);
    }

    /**
     * Best-effort max follower lag (commit − matchIndex) when this node is leader; {@code 0} on
     * followers / when commit-infos are unavailable.
     */
    static long maxFollowerLag(RaftMetadataClient client) {
        try {
            RaftServer.Division division = divisionOrNull(client);
            if (division == null || !division.getInfo().isLeader()) {
                return 0L;
            }
            long commit = division.getRaftLog().getLastCommittedIndex();
            RaftPeerId self = division.getId();
            long maxLag = 0L;
            for (CommitInfoProto info : division.getCommitInfos()) {
                if (!info.hasServer()) {
                    continue;
                }
                RaftPeerId peerId = RaftPeerId.valueOf(info.getServer().getId());
                if (peerId.equals(self)) {
                    continue;
                }
                maxLag = Math.max(maxLag, Math.max(0L, commit - info.getCommitIndex()));
            }
            return maxLag;
        } catch (Exception e) {
            LOG.debug("Could not read raft follower lag", e);
            return 0L;
        }
    }

    private static DivisionInfo infoOrNull(RaftMetadataClient client) {
        RaftServer.Division division = divisionOrNull(client);
        return division == null ? null : division.getInfo();
    }

    private static RaftServer.Division divisionOrNull(RaftMetadataClient client) {
        try {
            RaftMetadataServer server = client.ownedServer();
            if (server.getRaftServer() == null) {
                return null;
            }
            return server.getRaftServer().getDivision(RaftMetadataServer.RAFT_GROUP_ID);
        } catch (Exception e) {
            LOG.debug("Could not access raft division for metrics", e);
            return null;
        }
    }
}
