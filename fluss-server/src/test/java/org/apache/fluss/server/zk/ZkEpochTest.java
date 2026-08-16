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

package org.apache.fluss.server.zk;

import org.apache.fluss.server.coordinator.CoordinatorEpoch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test that {@link ZkEpoch} satisfies the backend-neutral {@link CoordinatorEpoch} contract. */
class ZkEpochTest {

    @Test
    void testCoordinatorEpochReflectsEpochValue() {
        ZkEpoch epoch = new ZkEpoch(5, 2);
        CoordinatorEpoch asNeutral = epoch;
        assertThat(asNeutral.coordinatorEpoch()).isEqualTo(5);
        assertThat(epoch.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(epoch.getCoordinatorEpochZkVersion()).isEqualTo(2);
    }

    @Test
    void testNextZkEpochAdvancesBothCounters() {
        ZkEpoch next = new ZkEpoch(5, 2).nextZkEpoch();
        assertThat(next.coordinatorEpoch()).isEqualTo(6);
        assertThat(next.getCoordinatorEpochZkVersion()).isEqualTo(3);
    }
}
