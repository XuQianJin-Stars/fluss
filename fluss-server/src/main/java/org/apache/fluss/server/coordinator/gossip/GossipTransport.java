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

import java.io.Closeable;
import java.io.IOException;

/**
 * Push-pull membership exchange. The caller delivers {@code localView} to {@code peer} and receives
 * that peer's view in return.
 */
@Internal
public interface GossipTransport extends Closeable {

    /**
     * Exchange views with {@code peer}.
     *
     * @param peer target gossip socket
     * @param localView snapshot of the caller's membership
     * @return the peer's membership after it merged {@code localView}
     */
    GossipView exchange(GossipMember peer, GossipView localView) throws IOException;

    @Override
    void close() throws IOException;
}
