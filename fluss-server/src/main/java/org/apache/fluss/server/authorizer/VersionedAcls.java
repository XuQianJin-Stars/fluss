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

package org.apache.fluss.server.authorizer;

import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.Set;

/**
 * A backend-neutral wrapper that holds a set of {@link AccessControlEntry} objects along with the
 * version of the record they were read from.
 *
 * <p>On the ZooKeeper backend the version is the ACL znode's CAS version; on the Raft backend the
 * per-record CAS is subsumed by the single-writer leader so the version is always {@code 0} when
 * present (see {@code zookeeper-removal-design.md} §5.1.6). In both backends an absent record is
 * represented by {@link ZooKeeperClient#UNKNOWN_VERSION}, which {@link #exists()} checks.
 */
public class VersionedAcls {
    public Set<AccessControlEntry> acls;
    int zkVersion;

    public VersionedAcls(int zkVersion, Set<AccessControlEntry> acls) {
        this.zkVersion = zkVersion;
        this.acls = acls;
    }

    public boolean exists() {
        return zkVersion != ZooKeeperClient.UNKNOWN_VERSION;
    }

    @Override
    public String toString() {
        return "VersionedAcls{" + "acls=" + acls + ", zkVersion=" + zkVersion + '}';
    }
}
