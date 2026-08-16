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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.metadata.TableBucket;

/**
 * Tuple of a table bucket and its current remote log manifest handle. Lives in the backend-neutral
 * {@code zk.data} package so it can be referenced from the {@code MetadataStoreClient} interface
 * and both its implementations ({@code ZooKeeperClient}, {@code RaftMetadataClient}) without
 * coupling the interface to a concrete backend.
 */
public final class TableBucketAndManifest {
    private final TableBucket tableBucket;
    private final RemoteLogManifestHandle manifestHandle;

    public TableBucketAndManifest(TableBucket tableBucket, RemoteLogManifestHandle manifestHandle) {
        this.tableBucket = tableBucket;
        this.manifestHandle = manifestHandle;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public RemoteLogManifestHandle getManifestHandle() {
        return manifestHandle;
    }
}
