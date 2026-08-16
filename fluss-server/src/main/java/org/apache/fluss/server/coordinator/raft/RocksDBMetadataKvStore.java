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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * RocksDB-backed {@link MetadataKvStore} for the production Raft coordinator path (§5.2.2 /
 * §5.2.5).
 *
 * <p>This is a thin binding under {@code coordinator.raft.dir/metadata-kv/} — deliberately
 * independent of the table-oriented {@code RocksDBKv} stack (write-batch backpressure, tablet
 * directories, etc.). Writes arrive only from the single-threaded state-machine apply loop.
 *
 * <p>Keys are ordered by RocksDB's default unsigned lexicographic comparator, matching {@link
 * InMemoryMetadataKvStore} so {@link #scanPrefix} returns a contiguous range.
 */
@Internal
@NotThreadSafe
public class RocksDBMetadataKvStore implements MetadataKvStore {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetadataKvStore.class);

    /** Subdirectory under {@code coordinator.raft.dir} that holds this store's RocksDB files. */
    public static final String SUBDIR = "metadata-kv";

    private final File dbPath;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final WriteOptions writeOptions;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;
    private final RocksDB db;

    private volatile boolean closed;

    private RocksDBMetadataKvStore(
            File dbPath,
            DBOptions dbOptions,
            ColumnFamilyOptions columnFamilyOptions,
            WriteOptions writeOptions,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            RocksDB db) {
        this.dbPath = dbPath;
        this.dbOptions = dbOptions;
        this.columnFamilyOptions = columnFamilyOptions;
        this.writeOptions = writeOptions;
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.db = db;
    }

    /**
     * Opens (or creates) a RocksDB instance at {@code raftDir}/{@value #SUBDIR}.
     *
     * @param raftDir value of {@code coordinator.raft.dir}
     */
    public static RocksDBMetadataKvStore open(File raftDir) throws IOException {
        if (raftDir == null) {
            throw new IllegalArgumentException("raftDir must not be null");
        }
        File kvDir = new File(raftDir, SUBDIR);
        if (!kvDir.exists() && !kvDir.mkdirs()) {
            throw new IOException("Could not create metadata KvStore directory at " + kvDir);
        }
        if (!kvDir.isDirectory()) {
            throw new IOException("metadata KvStore path is not a directory: " + kvDir);
        }

        RocksDB.loadLibrary();

        DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        WriteOptions writeOptions = new WriteOptions();
        ColumnFamilyHandle defaultCf = null;
        RocksDB db = null;
        try {
            List<ColumnFamilyDescriptor> descriptors =
                    Collections.singletonList(
                            new ColumnFamilyDescriptor(
                                    RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
            List<ColumnFamilyHandle> handles = new ArrayList<>(1);
            db = RocksDB.open(dbOptions, kvDir.getAbsolutePath(), descriptors, handles);
            defaultCf = handles.get(0);
            LOG.info("Opened RocksDB metadata KvStore at {}", kvDir.getAbsolutePath());
            return new RocksDBMetadataKvStore(
                    kvDir, dbOptions, columnFamilyOptions, writeOptions, defaultCf, db);
        } catch (RocksDBException e) {
            IOUtils.closeQuietly(defaultCf);
            IOUtils.closeQuietly(db);
            IOUtils.closeQuietly(writeOptions);
            IOUtils.closeQuietly(columnFamilyOptions);
            IOUtils.closeQuietly(dbOptions);
            throw new IOException("Failed to open RocksDB metadata KvStore at " + kvDir, e);
        }
    }

    /** Absolute path of the RocksDB directory (under {@code coordinator.raft.dir/metadata-kv}). */
    public File dbPath() {
        return dbPath;
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
        ensureOpen();
        try {
            return db.get(defaultColumnFamilyHandle, key);
        } catch (RocksDBException e) {
            throw new FlussRuntimeException("Failed to get key from RocksDB metadata KvStore", e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        ensureOpen();
        try {
            db.put(defaultColumnFamilyHandle, writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new FlussRuntimeException("Failed to put key into RocksDB metadata KvStore", e);
        }
    }

    @Override
    public void delete(byte[] key) {
        ensureOpen();
        try {
            db.delete(defaultColumnFamilyHandle, writeOptions, key);
        } catch (RocksDBException e) {
            throw new FlussRuntimeException(
                    "Failed to delete key from RocksDB metadata KvStore", e);
        }
    }

    @Override
    public List<byte[]> scanPrefix(byte[] prefix) {
        List<byte[][]> entries = scanPrefixEntries(prefix);
        List<byte[]> result = new ArrayList<>(entries.size());
        for (byte[][] entry : entries) {
            result.add(entry[1]);
        }
        return result;
    }

    @Override
    public List<byte[][]> scanPrefixEntries(byte[] prefix) {
        ensureOpen();
        List<byte[][]> result = new ArrayList<>();
        try (RocksIterator it = db.newIterator(defaultColumnFamilyHandle)) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] key = it.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                result.add(new byte[][] {key, it.value()});
                it.next();
            }
        }
        return result;
    }

    @Override
    public List<byte[][]> entries() {
        ensureOpen();
        List<byte[][]> result = new ArrayList<>();
        try (RocksIterator it = db.newIterator(defaultColumnFamilyHandle)) {
            it.seekToFirst();
            while (it.isValid()) {
                result.add(new byte[][] {it.key(), it.value()});
                it.next();
            }
        }
        return result;
    }

    @Override
    public void clear() {
        ensureOpen();
        // Collect keys first so deletes do not invalidate the iterator mid-scan.
        List<byte[]> keys = new ArrayList<>();
        try (RocksIterator it = db.newIterator(defaultColumnFamilyHandle)) {
            it.seekToFirst();
            while (it.isValid()) {
                keys.add(it.key());
                it.next();
            }
        }
        for (byte[] key : keys) {
            delete(key);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeQuietly(db);
        IOUtils.closeQuietly(writeOptions);
        IOUtils.closeQuietly(columnFamilyOptions);
        IOUtils.closeQuietly(dbOptions);
        LOG.info("Closed RocksDB metadata KvStore at {}", dbPath.getAbsolutePath());
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(
                    "RocksDB metadata KvStore is already closed: " + dbPath);
        }
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
