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

package org.apache.fluss.server.coordinator.gossip;

import org.apache.fluss.annotation.Internal;

import java.util.Objects;

/**
 * One coordinator advertised over gossip (§1.2 / §5.4.4). {@code rpcHost:rpcPort} is the {@code
 * coordinator.seed.nodes} address; {@code gossipHost:gossipPort} is the membership-exchange socket.
 * {@code heartbeat} is a monotonic counter the owner increments each round; {@code incarnation} is
 * bumped on restart so a stale heartbeat cannot overwrite a live node.
 */
@Internal
public final class GossipMember {

    private final String nodeId;
    private final String rpcHost;
    private final int rpcPort;
    private final String gossipHost;
    private final int gossipPort;
    private final long heartbeat;
    private final int incarnation;

    /**
     * Creates a gossip member.
     *
     * @param nodeId stable identity
     * @param rpcHost coordinator RPC host
     * @param rpcPort coordinator RPC port
     * @param gossipHost gossip listen host
     * @param gossipPort gossip listen port
     * @param heartbeat owner heartbeat counter
     * @param incarnation restart generation
     */
    public GossipMember(
            String nodeId,
            String rpcHost,
            int rpcPort,
            String gossipHost,
            int gossipPort,
            long heartbeat,
            int incarnation) {
        this.nodeId = nodeId;
        this.rpcHost = rpcHost;
        this.rpcPort = rpcPort;
        this.gossipHost = gossipHost;
        this.gossipPort = gossipPort;
        this.heartbeat = heartbeat;
        this.incarnation = incarnation;
    }

    /**
     * Contact-only member used to reach a seed before its real identity is known. Not inserted into
     * the local view.
     */
    public static GossipMember contact(String gossipHost, int gossipPort) {
        return new GossipMember(
                "contact-" + gossipHost + "-" + gossipPort,
                gossipHost,
                0,
                gossipHost,
                gossipPort,
                0L,
                0);
    }

    /** Stable node identity. */
    public String nodeId() {
        return nodeId;
    }

    /** Coordinator RPC host. */
    public String rpcHost() {
        return rpcHost;
    }

    /** Coordinator RPC port. */
    public int rpcPort() {
        return rpcPort;
    }

    /** Gossip listen host. */
    public String gossipHost() {
        return gossipHost;
    }

    /** Gossip listen port. */
    public int gossipPort() {
        return gossipPort;
    }

    /** Owner heartbeat counter. */
    public long heartbeat() {
        return heartbeat;
    }

    /** Restart generation. */
    public int incarnation() {
        return incarnation;
    }

    /** RPC seed-list entry ({@code host:port}). */
    public String rpcAddress() {
        return rpcHost + ":" + rpcPort;
    }

    /** Gossip socket ({@code host:port}). */
    public String gossipAddress() {
        return gossipHost + ":" + gossipPort;
    }

    /** Copy with a bound gossip port. */
    public GossipMember withGossipPort(int newGossipPort) {
        return new GossipMember(
                nodeId, rpcHost, rpcPort, gossipHost, newGossipPort, heartbeat, incarnation);
    }

    /** Copy with an updated heartbeat. */
    public GossipMember withHeartbeat(long newHeartbeat) {
        return new GossipMember(
                nodeId, rpcHost, rpcPort, gossipHost, gossipPort, newHeartbeat, incarnation);
    }

    /**
     * True when {@code incoming} should replace this member: higher incarnation, or the same
     * incarnation with a heartbeat that is at least as new.
     */
    public boolean isStaleComparedTo(GossipMember incoming) {
        if (incoming.incarnation > incarnation) {
            return true;
        }
        return incoming.incarnation == incarnation && incoming.heartbeat >= heartbeat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GossipMember that = (GossipMember) o;
        return rpcPort == that.rpcPort
                && gossipPort == that.gossipPort
                && heartbeat == that.heartbeat
                && incarnation == that.incarnation
                && Objects.equals(nodeId, that.nodeId)
                && Objects.equals(rpcHost, that.rpcHost)
                && Objects.equals(gossipHost, that.gossipHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                nodeId, rpcHost, rpcPort, gossipHost, gossipPort, heartbeat, incarnation);
    }

    @Override
    public String toString() {
        return "GossipMember{id="
                + nodeId
                + ", rpc="
                + rpcAddress()
                + ", gossip="
                + gossipAddress()
                + ", hb="
                + heartbeat
                + ", inc="
                + incarnation
                + '}';
    }
}
