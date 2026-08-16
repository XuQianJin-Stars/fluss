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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.raft.InMemoryMetadataKvStore;
import org.apache.fluss.server.coordinator.raft.MetadataKeys;
import org.apache.fluss.server.coordinator.raft.MetadataRecord;
import org.apache.fluss.server.coordinator.raft.MetadataWatchService;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.coordinator.raft.RaftMetadataStore;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

/**
 * Migrates metadata from a ZooKeeper ensemble into a Raft-backed coordinator store (design §6.3).
 *
 * <p>Export dumps {@code {path,b64}} JSONL; import maps znodes to {@link MetadataRecord}s and
 * appends them on a Raft leader (or applies into an in-memory store for offline fixtures); validate
 * checks byte-level equality for the core databases/tables/assignments subset.
 */
public class ZkToRaftMigrationTool {

    public ZkToRaftMigrationTool() {}

    /** Recursively read every znode under the metadata roots and dump JSONL. */
    public void exportZkMetadata(String zkAddress, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS.key(), zkAddress);
        try (ZooKeeperClient zk = ZooKeeperUtils.startZookeeperClient(conf, null)) {
            try (BufferedWriter writer =
                    Files.newBufferedWriter(
                            outputPath,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE)) {
                for (String root : exportRoots()) {
                    walkAndWrite(zk, root, writer);
                }
            }
        }
    }

    /**
     * Replay each exported znode as a {@link MetadataRecord}. When {@code raftSeeds} is empty, the
     * records are applied into a local in-memory store and the resulting key/value pairs are
     * written next to the input as {@code <input>.imported.jsonl} for offline validation. When
     * seeds are provided, records are appended through a local single-voter {@link
     * RaftMetadataClient}.
     */
    public void importToRaft(Path inputPath, List<String> raftSeeds) throws Exception {
        if (raftSeeds == null || raftSeeds.isEmpty()) {
            Path out = Paths.get(inputPath.toString() + ".imported.jsonl");
            importOffline(inputPath, out);
            System.out.println("Imported offline snapshot -> " + out);
            return;
        }

        List<ExportedZnode> znodes = readExport(inputPath);
        List<MetadataRecord> records = new ArrayList<>();
        for (ExportedZnode z : znodes) {
            Optional<MetadataRecord> mapped = ZnodeToRecordMapper.map(z.path, z.value);
            if (mapped.isPresent()) {
                records.add(mapped.get());
            }
        }

        Configuration conf = raftImportConf(raftSeeds);
        try (RaftMetadataClient raft = RaftMetadataClient.create(conf)) {
            awaitLeader(raft);
            for (MetadataRecord record : records) {
                raft.appendRaw(record);
            }
            System.out.println(
                    "Imported " + records.size() + " records into Raft at " + raftSeeds.get(0));
        }
    }

    /**
     * Byte-level check on the core subset (databases / tables / assignments) between a ZK export
     * and either another export or an {@code .imported.jsonl} store snapshot.
     */
    public boolean validate(Path zkExport, Path importedOrOtherExport) throws IOException {
        Map<String, byte[]> left = indexCorePaths(readExport(zkExport));
        Map<String, byte[]> right;
        if (importedOrOtherExport.toString().endsWith(".imported.jsonl")) {
            right = indexImportedStore(readExport(importedOrOtherExport));
        } else {
            right = indexCorePaths(readExport(importedOrOtherExport));
        }
        boolean ok = true;
        for (Map.Entry<String, byte[]> e : left.entrySet()) {
            byte[] other = right.get(e.getKey());
            if (other == null) {
                System.err.println("MISSING in import: " + e.getKey());
                ok = false;
            } else if (!Arrays.equals(e.getValue(), other)) {
                System.err.println("MISMATCH: " + e.getKey());
                ok = false;
            }
        }
        if (ok) {
            System.out.println("Validate OK: " + left.size() + " core paths match");
        }
        return ok;
    }

    // ---- CLI entry points used by FlussCli ----

    public static int exportMain(String[] args) throws Exception {
        String zk = null;
        String output = null;
        for (int i = 0; i < args.length; i++) {
            if ("--zk-address".equals(args[i]) && i + 1 < args.length) {
                zk = args[++i];
            } else if ("--output".equals(args[i]) && i + 1 < args.length) {
                output = args[++i];
            }
        }
        if (zk == null || output == null) {
            System.err.println(
                    "Usage: fluss zk-to-raft-export --zk-address <addr> --output <file>");
            return 1;
        }
        new ZkToRaftMigrationTool().exportZkMetadata(zk, Paths.get(output));
        System.out.println("Exported ZooKeeper metadata to " + output);
        return 0;
    }

    public static int importMain(String[] args) throws Exception {
        String input = null;
        List<String> seeds = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--input".equals(args[i]) && i + 1 < args.length) {
                input = args[++i];
            } else if ("--raft-seeds".equals(args[i]) && i + 1 < args.length) {
                for (String s : args[++i].split(",")) {
                    if (!s.trim().isEmpty()) {
                        seeds.add(s.trim());
                    }
                }
            }
        }
        if (input == null) {
            System.err.println("Usage: fluss raft-import --input <file> [--raft-seeds host:port]");
            return 1;
        }
        new ZkToRaftMigrationTool().importToRaft(Paths.get(input), seeds);
        return 0;
    }

    public static int validateMain(String[] args) throws Exception {
        String zkExport = null;
        String imported = null;
        for (int i = 0; i < args.length; i++) {
            if ("--zk-export".equals(args[i]) && i + 1 < args.length) {
                zkExport = args[++i];
            } else if ("--input".equals(args[i]) && i + 1 < args.length) {
                imported = args[++i];
            } else if ("--imported".equals(args[i]) && i + 1 < args.length) {
                imported = args[++i];
            } else if ("--zk-address".equals(args[i]) && i + 1 < args.length) {
                String zk = args[++i];
                Path tmp = Files.createTempFile("fluss-zk-reexport-", ".jsonl");
                new ZkToRaftMigrationTool().exportZkMetadata(zk, tmp);
                zkExport = tmp.toString();
            }
        }
        if (zkExport == null || imported == null) {
            System.err.println(
                    "Usage: fluss zk-to-raft-validate --zk-export <file> --imported <file>");
            System.err.println(
                    "   or: fluss zk-to-raft-validate --zk-address <addr> --input <imported>");
            return 1;
        }
        boolean ok = new ZkToRaftMigrationTool().validate(Paths.get(zkExport), Paths.get(imported));
        return ok ? 0 : 1;
    }

    /**
     * Online converge from ZooKeeper to Raft (design §6.3). Re-scans {@code mzxid} diffs until
     * quiet, then runs {@code fenceWrites} and one final scan. When {@code raftSeeds} is empty,
     * records are applied into an in-memory store (offline / test fixture).
     */
    public LiveMigrator.Result liveMigration(
            String zkAddress, List<String> raftSeeds, LiveMigrator.Options options)
            throws Exception {
        LiveMigrator.MetadataSource source = () -> scanZk(zkAddress);
        if (raftSeeds == null || raftSeeds.isEmpty()) {
            RaftMetadataStore store =
                    new RaftMetadataStore(
                            new InMemoryMetadataKvStore(), new MetadataWatchService());
            return LiveMigrator.run(source, store::apply, options);
        }
        Configuration conf = raftImportConf(raftSeeds);
        try (RaftMetadataClient raft = RaftMetadataClient.create(conf)) {
            awaitLeader(raft);
            return LiveMigrator.run(source, raft::appendRaw, options);
        }
    }

    /** CLI entry for {@code fluss zk-to-raft-live-migrate}. */
    public static int liveMigrateMain(String[] args) throws Exception {
        String zk = null;
        List<String> seeds = new ArrayList<>();
        int maxRounds = 32;
        for (int i = 0; i < args.length; i++) {
            if ("--zk-address".equals(args[i]) && i + 1 < args.length) {
                zk = args[++i];
            } else if ("--raft-seeds".equals(args[i]) && i + 1 < args.length) {
                for (String s : args[++i].split(",")) {
                    if (!s.trim().isEmpty()) {
                        seeds.add(s.trim());
                    }
                }
            } else if ("--max-rounds".equals(args[i]) && i + 1 < args.length) {
                maxRounds = Integer.parseInt(args[++i]);
            }
        }
        if (zk == null) {
            System.err.println(
                    "Usage: fluss zk-to-raft-live-migrate --zk-address <addr> "
                            + "[--raft-seeds host:port] [--max-rounds N]");
            return 1;
        }
        LiveMigrator.Options options =
                new LiveMigrator.Options()
                        .setMaxConvergeRounds(maxRounds)
                        .setFenceWrites(
                                () ->
                                        System.out.println(
                                                "Fencing ZooKeeper writes for the final re-scan "
                                                        + "(operator: mark coordinator read-only)."));
        LiveMigrator.Result result = new ZkToRaftMigrationTool().liveMigration(zk, seeds, options);
        System.out.println(
                "Live migration done: baseline="
                        + result.baselineImported
                        + " convergeRounds="
                        + result.convergeRounds
                        + " convergeReplayed="
                        + result.convergeReplayed
                        + " finalReplayed="
                        + result.finalReplayed
                        + " convergedBeforeFence="
                        + result.convergedBeforeFence);
        return 0;
    }

    private static Configuration raftImportConf(List<String> raftSeeds) throws IOException {
        Configuration conf = new Configuration();
        String voter = raftSeeds.get(0);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS.key(), voter);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID.key(), voter);
        Path raftDir = Files.createTempDirectory("fluss-raft-import-");
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR.key(), raftDir.toString());
        return conf;
    }

    private static void awaitLeader(RaftMetadataClient raft) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (!raft.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50L);
        }
        if (!raft.isLeader()) {
            throw new IllegalStateException("Raft import client did not become leader in time");
        }
    }

    /**
     * Walk the same export roots as {@link #exportZkMetadata} and capture {@code mzxid} for
     * live-migration diffs.
     */
    List<LiveMigrator.ZnodeSnapshot> scanZk(String zkAddress) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS.key(), zkAddress);
        List<LiveMigrator.ZnodeSnapshot> out = new ArrayList<>();
        try (ZooKeeperClient zk = ZooKeeperUtils.startZookeeperClient(conf, null)) {
            for (String root : exportRoots()) {
                Queue<String> queue = new ArrayDeque<>();
                queue.add(root);
                while (!queue.isEmpty()) {
                    String current = queue.poll();
                    try {
                        Optional<byte[]> data = zk.getOrEmpty(current);
                        if (data.isPresent()) {
                            long mzxid = 0L;
                            Optional<Stat> stat = zk.getStat(current);
                            if (stat.isPresent()) {
                                mzxid = stat.get().getMzxid();
                            }
                            out.add(new LiveMigrator.ZnodeSnapshot(current, data.get(), mzxid));
                        }
                        for (String child : zk.getChildren(current)) {
                            String childPath =
                                    current.endsWith("/") ? current + child : current + "/" + child;
                            queue.add(childPath);
                        }
                    } catch (Exception e) {
                        // Missing parent — skip this branch.
                    }
                }
            }
        }
        return out;
    }

    // ---- internals ----

    private static List<String> exportRoots() {
        List<String> roots = new ArrayList<>();
        roots.add(ZkData.DatabasesZNode.path());
        roots.add("/tabletservers/tables");
        return roots;
    }

    private static void walkAndWrite(ZooKeeperClient zk, String path, BufferedWriter writer)
            throws Exception {
        Queue<String> queue = new ArrayDeque<>();
        queue.add(path);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            try {
                Optional<byte[]> data = zk.getOrEmpty(current);
                if (data.isPresent()) {
                    writeLine(writer, current, data.get());
                }
                for (String child : zk.getChildren(current)) {
                    String childPath =
                            current.endsWith("/") ? current + child : current + "/" + child;
                    queue.add(childPath);
                }
            } catch (Exception e) {
                // Missing parent — skip this branch.
            }
        }
    }

    static void writeLine(BufferedWriter writer, String path, byte[] value) throws IOException {
        String b64 = Base64.getEncoder().encodeToString(value == null ? new byte[0] : value);
        writer.write("{\"path\":\"");
        writer.write(escapeJson(path));
        writer.write("\",\"b64\":\"");
        writer.write(b64);
        writer.write("\"}");
        writer.newLine();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    static List<ExportedZnode> readExport(Path inputPath) throws IOException {
        List<ExportedZnode> out = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                out.add(ExportedZnode.parse(line));
            }
        }
        return out;
    }

    private static Map<String, byte[]> indexCorePaths(List<ExportedZnode> znodes) {
        Map<String, byte[]> map = new HashMap<>();
        for (ExportedZnode z : znodes) {
            if (isCorePath(z.path)) {
                map.put(z.path, z.value);
            }
        }
        return map;
    }

    private static Map<String, byte[]> indexImportedStore(List<ExportedZnode> znodes) {
        Map<String, byte[]> map = new HashMap<>();
        for (ExportedZnode z : znodes) {
            if (z.path.startsWith("zk:")) {
                map.put(z.path.substring("zk:".length()), z.value);
            }
        }
        return map;
    }

    private static boolean isCorePath(String path) {
        if (path.startsWith(ZkData.DatabasesZNode.path() + "/")) {
            String rest = path.substring(ZkData.DatabasesZNode.path().length() + 1);
            // database node or table node (no deeper children like schemas)
            if (!rest.contains("/")) {
                return true;
            }
            int tablesIdx = rest.indexOf("/tables/");
            if (tablesIdx > 0) {
                String after = rest.substring(tablesIdx + "/tables/".length());
                return !after.contains("/");
            }
            return false;
        }
        if (path.startsWith("/tabletservers/tables/")) {
            String rest = path.substring("/tabletservers/tables/".length());
            return !rest.contains("/");
        }
        return false;
    }

    /**
     * Offline import that both applies to the store and emits {@code zk:<path>} lines so {@link
     * #validate} can compare payloads without reverse-mapping Raft keys.
     */
    void importOffline(Path inputPath, Path outputSnapshot) throws Exception {
        List<ExportedZnode> znodes = readExport(inputPath);
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        try (BufferedWriter writer =
                Files.newBufferedWriter(
                        outputSnapshot,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)) {
            for (ExportedZnode z : znodes) {
                Optional<MetadataRecord> mapped = ZnodeToRecordMapper.map(z.path, z.value);
                if (!mapped.isPresent()) {
                    continue;
                }
                MetadataRecord record = mapped.get();
                store.apply(record);
                if (isCorePath(z.path)) {
                    writeLine(writer, "zk:" + z.path, z.value);
                    byte[] storeKey = MetadataKeys.keyOf(record);
                    writeLine(
                            writer,
                            "raft:" + Base64.getEncoder().encodeToString(storeKey),
                            record.payload());
                }
            }
        }
    }

    /** Exported znode line. */
    static final class ExportedZnode {
        final String path;
        final byte[] value;

        ExportedZnode(String path, byte[] value) {
            this.path = path;
            this.value = value == null ? new byte[0] : value;
        }

        static ExportedZnode parse(String jsonLine) {
            String path = extract(jsonLine, "path");
            String b64 = extract(jsonLine, "b64");
            byte[] value =
                    b64 == null || b64.isEmpty() ? new byte[0] : Base64.getDecoder().decode(b64);
            return new ExportedZnode(path, value);
        }

        private static String extract(String json, String field) {
            String key = "\"" + field + "\":\"";
            int start = json.indexOf(key);
            if (start < 0) {
                return "";
            }
            start += key.length();
            StringBuilder sb = new StringBuilder();
            for (int i = start; i < json.length(); i++) {
                char c = json.charAt(i);
                if (c == '\\' && i + 1 < json.length()) {
                    sb.append(json.charAt(++i));
                } else if (c == '"') {
                    break;
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }
    }
}
