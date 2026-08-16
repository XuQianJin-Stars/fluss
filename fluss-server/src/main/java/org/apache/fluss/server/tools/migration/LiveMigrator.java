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

package org.apache.fluss.server.tools.migration;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.coordinator.raft.MetadataRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntConsumer;

/**
 * Online ZK→Raft converge loop (design §6.3 {@code liveMigration}).
 *
 * <p>ZK watches are edge-triggered and coalescing, so this does <em>not</em> use watches. Instead:
 *
 * <ol>
 *   <li>Baseline import: full scan, append every mapped znode, record {@code mzxid}.
 *   <li>Converge: repeated full re-scan; replay znodes whose {@code mzxid} advanced (or that
 *       appeared/disappeared). Repeat until a scan observes zero diffs or {@code maxConvergeRounds}
 *       is hit (then fall back to the fenced final scan).
 *   <li>Fence writes, one final re-scan, replay leftovers.
 * </ol>
 *
 * <p>The write-fence is supplied by the caller so tests can freeze an in-memory source and the
 * production path can mark the ZK coordinator read-only (or rely on an operator fence).
 */
@Internal
public final class LiveMigrator {

    private static final Logger LOG = LoggerFactory.getLogger(LiveMigrator.class);

    private LiveMigrator() {}

    /** One znode observed in a scan. */
    public static final class ZnodeSnapshot {
        public final String path;
        public final byte[] value;
        public final long mzxid;

        /** Creates a scan snapshot for {@code path}. */
        public ZnodeSnapshot(String path, byte[] value, long mzxid) {
            this.path = path;
            this.value = value == null ? new byte[0] : value;
            this.mzxid = mzxid;
        }
    }

    /** Full recursive listing of metadata znodes with {@code mzxid}. */
    public interface MetadataSource {
        /**
         * Scan the source metadata tree.
         *
         * @return all znodes under the export roots
         */
        List<ZnodeSnapshot> scan() throws Exception;
    }

    /** Append a mapped {@link MetadataRecord} (Raft leader or in-memory store). */
    public interface RecordSink {
        /**
         * Append one mapped record.
         *
         * @param record mapped metadata mutation
         */
        void append(MetadataRecord record) throws Exception;
    }

    /** Tunables and hooks for the four-step protocol. */
    public static final class Options {
        private int maxConvergeRounds = 32;
        private long roundIntervalMs = 0L;
        private Runnable fenceWrites = () -> {};
        private IntConsumer beforeRound = round -> {};

        /** Upper bound on live re-scan rounds before falling through to the fenced scan. */
        public Options setMaxConvergeRounds(int maxConvergeRounds) {
            this.maxConvergeRounds = maxConvergeRounds;
            return this;
        }

        /** Pause between re-scan rounds; {@code 0} means no pause. */
        public Options setRoundIntervalMs(long roundIntervalMs) {
            this.roundIntervalMs = roundIntervalMs;
            return this;
        }

        /** Invoked once after converge (or max rounds) to stop ZK writes. */
        public Options setFenceWrites(Runnable fenceWrites) {
            this.fenceWrites = fenceWrites;
            return this;
        }

        /** Test hook invoked at the start of each converge round (1-based). */
        public Options setBeforeRound(IntConsumer beforeRound) {
            this.beforeRound = beforeRound;
            return this;
        }
    }

    /** Outcome of {@link #run}. */
    public static final class Result {
        public final int baselineImported;
        public final int convergeRounds;
        public final int convergeReplayed;
        public final int finalReplayed;
        public final boolean convergedBeforeFence;

        Result(
                int baselineImported,
                int convergeRounds,
                int convergeReplayed,
                int finalReplayed,
                boolean convergedBeforeFence) {
            this.baselineImported = baselineImported;
            this.convergeRounds = convergeRounds;
            this.convergeReplayed = convergeReplayed;
            this.finalReplayed = finalReplayed;
            this.convergedBeforeFence = convergedBeforeFence;
        }

        /** Total mapped records appended (baseline + converge + final). */
        public int totalAppended() {
            return baselineImported + convergeReplayed + finalReplayed;
        }
    }

    /**
     * Run the live-migration protocol against {@code source} / {@code sink}.
     *
     * @param source ZK (or in-memory) snapshot source
     * @param sink Raft append (or in-memory apply)
     * @param options round limits, fence, and test hooks
     * @return counts for tests / CLI
     */
    public static Result run(MetadataSource source, RecordSink sink, Options options)
            throws Exception {
        Options opts = options == null ? new Options() : options;
        Map<String, Long> baseline = new HashMap<>();
        Map<String, byte[]> payloads = new HashMap<>();

        List<ZnodeSnapshot> scan = source.scan();
        int baselineImported = replayDiffs(emptyBaseline(), scan, sink, baseline, payloads);
        LOG.info("Live migration baseline imported {} records.", baselineImported);

        int convergeReplayed = 0;
        int round = 0;
        boolean converged = false;
        while (round < opts.maxConvergeRounds) {
            round++;
            if (opts.beforeRound != null) {
                opts.beforeRound.accept(round);
            }
            scan = source.scan();
            int replayed = replayDiffs(copyOf(baseline), scan, sink, baseline, payloads);
            convergeReplayed += replayed;
            if (replayed == 0) {
                converged = true;
                LOG.info("Live migration converged after {} re-scan round(s).", round);
                break;
            }
            LOG.info(
                    "Live migration re-scan round {} replayed {} changed znode(s).",
                    round,
                    replayed);
            if (opts.roundIntervalMs > 0L) {
                Thread.sleep(opts.roundIntervalMs);
            }
        }

        if (opts.fenceWrites != null) {
            opts.fenceWrites.run();
        }
        scan = source.scan();
        int finalReplayed = replayDiffs(copyOf(baseline), scan, sink, baseline, payloads);
        LOG.info(
                "Live migration final fenced scan replayed {} record(s); convergedBeforeFence={}.",
                finalReplayed,
                converged);
        return new Result(baselineImported, round, convergeReplayed, finalReplayed, converged);
    }

    private static Map<String, Long> emptyBaseline() {
        return new HashMap<>();
    }

    private static Map<String, Long> copyOf(Map<String, Long> baseline) {
        return new HashMap<>(baseline);
    }

    /**
     * Replay znodes that are new, deleted, or have a newer {@code mzxid} than {@code
     * previousBaseline}. Updates {@code baseline} / {@code payloads} in place.
     */
    static int replayDiffs(
            Map<String, Long> previousBaseline,
            List<ZnodeSnapshot> scan,
            RecordSink sink,
            Map<String, Long> baseline,
            Map<String, byte[]> payloads)
            throws Exception {
        int replayed = 0;
        Map<String, ZnodeSnapshot> current = new HashMap<>();
        for (ZnodeSnapshot z : scan) {
            current.put(z.path, z);
        }

        for (ZnodeSnapshot z : scan) {
            Long prev = previousBaseline.get(z.path);
            boolean changed =
                    prev == null || z.mzxid > prev || !Arrays.equals(payloads.get(z.path), z.value);
            if (!changed) {
                continue;
            }
            Optional<MetadataRecord> mapped = ZnodeToRecordMapper.map(z.path, z.value);
            if (mapped.isPresent()) {
                sink.append(mapped.get());
                replayed++;
            }
            baseline.put(z.path, z.mzxid);
            payloads.put(z.path, z.value);
        }

        List<String> deleted = new ArrayList<>();
        for (String path : previousBaseline.keySet()) {
            if (!current.containsKey(path)) {
                deleted.add(path);
            }
        }
        for (String path : deleted) {
            Optional<MetadataRecord> mapped = ZnodeToRecordMapper.mapDelete(path);
            if (mapped.isPresent()) {
                sink.append(mapped.get());
                replayed++;
            }
            baseline.remove(path);
            payloads.remove(path);
        }
        return replayed;
    }
}
