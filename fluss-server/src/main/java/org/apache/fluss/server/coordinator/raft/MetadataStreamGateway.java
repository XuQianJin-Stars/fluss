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

import java.io.IOException;

/**
 * Transport-neutral view of the leader-side metadata stream (§5.8), consumed by {@link
 * MetadataStreamSubscriber}. Decoupling the subscriber from the transport lets the same replay
 * logic run against either an in-process {@link MetadataStreamService} (tests / same-JVM standby)
 * or, once T5-1 lands, a Raft-RPC client that targets the current leader.
 *
 * <p>Implementations are leader-served: a call reaching a non-leader (or a stale leader) surfaces
 * the fencing epoch in the response ({@link MetadataStreamService.FetchResult#leaderEpoch()}) or
 * fails with {@link IOException} so the subscriber reconnects to the new leader.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public interface MetadataStreamGateway {

    /**
     * Fetches an ordered, contiguous run of committed metadata records starting at {@code
     * fromOffset}, long-polling up to {@code maxWaitMs} when the subscriber is caught up.
     */
    MetadataStreamService.FetchResult fetch(long fromOffset, int maxRecords, long maxWaitMs)
            throws IOException;

    /**
     * Fetches a full terminal-state snapshot for bootstrap when the requested offset was compacted
     * out of the log ({@link MetadataStreamService.FetchResult#snapshotRequired()}).
     */
    MetadataStreamService.FetchSnapshotResult fetchSnapshot() throws IOException;
}
