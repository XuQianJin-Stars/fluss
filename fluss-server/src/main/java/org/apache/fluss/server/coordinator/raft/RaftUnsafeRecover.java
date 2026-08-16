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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.protocol.TermIndex;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.segmented.LogSegmentStartEnd;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream;
import org.apache.fluss.shaded.ratis.org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Offline quorum-loss recovery (§9.4): force the on-disk Raft group configuration under {@code
 * coordinator.raft.dir} to the single surviving peer.
 *
 * <p>Mirrors the Ratis {@code local raftMetaConf} / Ozone unsafe-recovery path: rewrite {@code
 * raft-meta.conf} with a configuration entry whose log index is {@code max(confIndex, lastLogIndex)
 * + 1} (and whose term is at least the last log term) and whose peer set is the survivor only. Log
 * segments are removed so a still-committed multi-voter configuration cannot override the rewrite.
 * Unreplicated commits on lost voters may be discarded. This MUST never run automatically — callers
 * require an explicit operator acknowledgement.
 *
 * <p>The coordinator process using this Raft dir MUST be stopped before invoking recovery.
 */
@Internal
public final class RaftUnsafeRecover {

    static final String RAFT_META_CONF = "raft-meta.conf";
    static final String BOOTSTRAP_MARKER = "fluss-raft-bootstrap.marker";
    private static final String CURRENT_DIR = "current";

    private RaftUnsafeRecover() {}

    /**
     * Resolves the local survivor peer from configuration and rewrites Raft storage to a singleton
     * group.
     *
     * @return a short human-readable summary of what was rewritten
     */
    public static String recover(Configuration conf) throws IOException {
        String raftDirPath = conf.getOptional(ConfigOptions.COORDINATOR_RAFT_DIR).orElse(null);
        if (raftDirPath == null || raftDirPath.trim().isEmpty()) {
            throw new IllegalConfigurationException(
                    "coordinator.raft.dir must be set for raft unsafe-recover");
        }
        RaftPeer survivor = resolveSurvivorPeer(conf);
        return recover(new File(raftDirPath.trim()), survivor);
    }

    /**
     * Forces the Raft group under {@code raftDir} to {@code {survivor}} by rewriting {@code
     * raft-meta.conf} (and the Fluss bootstrap marker when present).
     */
    public static String recover(File raftDir, RaftPeer survivor) throws IOException {
        if (survivor == null || survivor.getId() == null || survivor.getAddress() == null) {
            throw new IllegalArgumentException("survivor peer id and address are required");
        }
        if (!raftDir.exists() || !raftDir.isDirectory()) {
            throw new IOException("Raft dir does not exist or is not a directory: " + raftDir);
        }

        File currentDir = locateCurrentDir(raftDir);
        File metaConf = new File(currentDir, RAFT_META_CONF);
        long previousIndex = 0L;
        long previousTerm = 0L;
        if (metaConf.isFile()) {
            LogEntryProto existing = readConfEntry(metaConf);
            previousIndex = existing.getIndex();
            previousTerm = existing.getTerm();
            Path backup =
                    metaConf.toPath()
                            .resolveSibling(RAFT_META_CONF + ".bak." + System.currentTimeMillis());
            Files.copy(metaConf.toPath(), backup, StandardCopyOption.REPLACE_EXISTING);
        } else if (!currentDir.exists() && !currentDir.mkdirs()) {
            throw new IOException("Cannot create Raft current dir: " + currentDir);
        }

        TermIndex lastLog = readLastLogTermIndex(currentDir);
        if (lastLog.getIndex() > previousIndex) {
            previousIndex = lastLog.getIndex();
            previousTerm = lastLog.getTerm();
        }
        long newIndex = previousIndex + 1;
        long newTerm = previousTerm > 0L ? previousTerm : 1L;
        LogEntryProto entry = buildSingletonConfEntry(survivor, newIndex, newTerm);
        // Membership is taken from committed log conf entries, which outrank raft-meta.conf.
        // After majority loss those log confs still require the lost peers; drop the segments so
        // the rewritten singleton raft-meta.conf becomes the effective group. Applied metadata
        // remains in the KvStore. Unreplicated log suffixes are discarded by design (§9.4).
        deleteLogSegments(currentDir);
        atomicWrite(metaConf.toPath(), entry);

        updateBootstrapMarker(raftDir, survivor.getAddress());

        return "Forced Raft group to single voter "
                + survivor.getAddress()
                + " (id="
                + survivor.getId()
                + ") at conf index "
                + newIndex
                + " under "
                + currentDir.getAbsolutePath();
    }

    /**
     * Local survivor identity: {@link ConfigOptions#COORDINATOR_RAFT_NODE_ID} when set; otherwise
     * the sole entry in {@link ConfigOptions#COORDINATOR_RAFT_VOTERS} (or the first voter when the
     * operator has already narrowed the list).
     */
    static RaftPeer resolveSurvivorPeer(Configuration conf) {
        String nodeId = conf.getOptional(ConfigOptions.COORDINATOR_RAFT_NODE_ID).orElse(null);
        String votersCsv = conf.getOptional(ConfigOptions.COORDINATOR_RAFT_VOTERS).orElse(null);
        List<String> voters = parseVoterAddresses(votersCsv);

        String address;
        if (nodeId != null && !nodeId.trim().isEmpty()) {
            String trimmed = nodeId.trim();
            // Accept host:port or host_port; prefer matching a voters entry for the real address.
            address = matchVoterAddress(trimmed, voters);
            if (address == null) {
                if (trimmed.indexOf(':') < 0) {
                    throw new IllegalConfigurationException(
                            "coordinator.raft.node.id must be host:port (or match a voters entry); got: "
                                    + trimmed);
                }
                address = trimmed;
            }
        } else if (voters.isEmpty()) {
            throw new IllegalConfigurationException(
                    "coordinator.raft.node.id or coordinator.raft.voters is required for unsafe-recover");
        } else {
            address = voters.get(0);
        }

        RaftPeerId peerId = RaftMetadataClient.peerIdForAddress(address);
        return RaftPeer.newBuilder().setId(peerId).setAddress(address).build();
    }

    private static String matchVoterAddress(String nodeIdOrAddress, List<String> voters) {
        RaftPeerId want = RaftMetadataClient.peerIdForAddress(nodeIdOrAddress);
        for (String voter : voters) {
            if (RaftMetadataClient.peerIdForAddress(voter).equals(want)) {
                return voter;
            }
        }
        return null;
    }

    private static List<String> parseVoterAddresses(String votersCsv) {
        List<String> out = new ArrayList<>();
        if (votersCsv == null || votersCsv.trim().isEmpty()) {
            return out;
        }
        for (String entry : votersCsv.split(",")) {
            String address = entry.trim();
            if (!address.isEmpty()) {
                out.add(address);
            }
        }
        return out;
    }

    /**
     * Prefers the fixed Fluss group UUID subdirectory; otherwise the first child of {@code raftDir}
     * that contains {@code current/} (or {@code current/raft-meta.conf}).
     */
    static File locateCurrentDir(File raftDir) throws IOException {
        File preferred =
                new File(
                        new File(raftDir, RaftMetadataServer.DEFAULT_GROUP_UUID.toString()),
                        CURRENT_DIR);
        if (preferred.isDirectory()
                || new File(preferred, RAFT_META_CONF).isFile()
                || new File(raftDir, BOOTSTRAP_MARKER).isFile()) {
            // Bootstrap-only dirs may not have created the group subdir yet; still prefer the
            // canonical path so recovery lays out storage where Ratis expects it.
            if (preferred.isDirectory() || new File(preferred, RAFT_META_CONF).isFile()) {
                return preferred;
            }
        }

        File[] children = raftDir.listFiles();
        if (children != null) {
            for (File child : children) {
                if (!child.isDirectory()) {
                    continue;
                }
                File current = new File(child, CURRENT_DIR);
                if (current.isDirectory() || new File(current, RAFT_META_CONF).isFile()) {
                    return current;
                }
            }
        }

        // No existing group storage — create under the fixed group id (first start after recover).
        if (!preferred.exists() && !preferred.mkdirs()) {
            throw new IOException("Cannot create Raft current dir: " + preferred);
        }
        return preferred;
    }

    private static LogEntryProto readConfEntry(File metaConf) throws IOException {
        try (InputStream in = Files.newInputStream(metaConf.toPath())) {
            return LogEntryProto.newBuilder().mergeFrom(in).build();
        }
    }

    /**
     * Highest term/index present in on-disk log segments. Open segments are scanned because their
     * filename does not encode the end index. Used so the rewritten singleton conf is strictly
     * newer than any existing log entry; otherwise Ratis keeps the old multi-voter conf and cannot
     * elect after majority loss.
     */
    static TermIndex readLastLogTermIndex(File currentDir) throws IOException {
        TermIndex last = TermIndex.valueOf(0L, 0L);
        File[] files = currentDir.listFiles();
        if (files == null) {
            return last;
        }
        for (File file : files) {
            LogSegmentPath segmentPath = LogSegmentPath.matchLogSegment(file.toPath());
            if (segmentPath == null) {
                continue;
            }
            LogSegmentStartEnd startEnd = segmentPath.getStartEnd();
            try (SegmentedRaftLogInputStream in =
                    new SegmentedRaftLogInputStream(
                            file,
                            startEnd.getStartIndex(),
                            startEnd.getEndIndex(),
                            startEnd.isOpen())) {
                LogEntryProto entry;
                while ((entry = in.nextEntry()) != null) {
                    if (entry.getIndex() >= last.getIndex()) {
                        last = TermIndex.valueOf(entry.getTerm(), entry.getIndex());
                    }
                }
            }
        }
        return last;
    }

    /**
     * Removes Ratis log segment files so a rewritten {@code raft-meta.conf} is not overridden by a
     * still-committed multi-voter configuration in the log.
     */
    private static void deleteLogSegments(File currentDir) throws IOException {
        File[] files = currentDir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (LogSegmentPath.matchLogSegment(file.toPath()) != null) {
                Files.deleteIfExists(file.toPath());
            }
        }
    }

    private static LogEntryProto buildSingletonConfEntry(RaftPeer survivor, long index, long term) {
        RaftPeerProto peerProto =
                RaftPeerProto.newBuilder()
                        .setId(
                                ByteString.copyFrom(
                                        survivor.getId()
                                                .toString()
                                                .getBytes(StandardCharsets.UTF_8)))
                        .setAddress(survivor.getAddress())
                        .setStartupRole(RaftPeerRole.FOLLOWER)
                        .build();
        return LogEntryProto.newBuilder()
                .setTerm(term)
                .setConfigurationEntry(
                        RaftConfigurationProto.newBuilder().addPeers(peerProto).build())
                .setIndex(index)
                .build();
    }

    private static void atomicWrite(Path target, LogEntryProto entry) throws IOException {
        Path parent = target.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
        Path tmp = target.resolveSibling(target.getFileName().toString() + ".tmp");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            entry.writeTo(out);
        }
        moveReplace(tmp, target);
    }

    private static void updateBootstrapMarker(File raftDir, String survivorAddress)
            throws IOException {
        File marker = new File(raftDir, BOOTSTRAP_MARKER);
        String content =
                "voters="
                        + survivorAddress
                        + "\n"
                        + "unsafe_recovered_at_ms="
                        + System.currentTimeMillis()
                        + "\n";
        Path tmp = marker.toPath().resolveSibling(BOOTSTRAP_MARKER + ".tmp");
        Files.write(tmp, content.getBytes(StandardCharsets.UTF_8));
        moveReplace(tmp, marker.toPath());
    }

    private static void moveReplace(Path tmp, Path target) throws IOException {
        try {
            Files.move(
                    tmp,
                    target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
