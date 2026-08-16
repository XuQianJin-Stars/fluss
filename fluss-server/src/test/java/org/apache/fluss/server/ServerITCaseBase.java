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

package org.apache.fluss.server;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcGateway;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.server.cli.CommandLineOptions;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.utils.TestProcessBuilder;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.CommonTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base integration process test for {@link CoordinatorServer} and {@link TabletServer}. */
public abstract class ServerITCaseBase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final String SERVER_STARTED_MARKER = "Successfully start Netty server";
    private static final String SERVER_FAILED_MARKER = "Could not start";
    private static final Pattern BIND_ENDPOINTS_PATTERN =
            Pattern.compile("Listening on endpoints \\[([^\\]]+)]");

    /** Bound node parsed from the spawned process log after Netty starts. */
    private ServerNode boundServerNode;

    /**
     * Template node used before the process binds. The port may be {@code 0} (ephemeral); the
     * actual port is taken from the process log after startup.
     */
    protected abstract ServerNode getServerNode();

    protected abstract Class<? extends RpcGateway> getRpcGatewayClass();

    protected abstract Class<? extends ServerBase> getServerClass();

    protected abstract Configuration getServerConfig();

    @Test
    void testRunServerUsingProcess(@TempDir Path tempFolder) throws Exception {
        Path yamlFile = tempFolder.resolve("server.yaml");
        generateYamlFile(yamlFile, getServerConfig());
        final Configuration configuration = new Configuration();
        configuration.setString(
                CommandLineOptions.CONFIG_DIR_OPTION.getLongOpt(),
                tempFolder.toAbsolutePath().toString());

        boolean success = false;
        TestProcessBuilder.TestProcess serverProcess = null;

        try {
            serverProcess =
                    new TestProcessBuilder(getServerClass().getName())
                            .addConfigAsMainClassArgs(configuration)
                            .addMainClassArg(
                                    String.format(
                                            "-D%s=%s",
                                            ConfigOptions.ZOOKEEPER_ADDRESS.key(),
                                            ZOO_KEEPER_EXTENSION_WRAPPER
                                                    .getCustomExtension()
                                                    .getConnectString()))
                            .start();

            // now, wait until server startup
            waitUntilServerStartup(serverProcess);

            // test connection
            testConnectionToServer();

            serverProcess.getProcess().destroy();

            serverProcess.getProcess().waitFor();

            success = true;
        } finally {
            if (serverProcess != null) {
                if (!success) {
                    TestProcessBuilder.TestProcess.printProcessLog(
                            getServerNode().serverType().toString(), serverProcess);
                }
                serverProcess.destroy();
            }
        }
    }

    private void waitUntilServerStartup(TestProcessBuilder.TestProcess process) {
        CommonTestUtils.waitUntil(
                () -> {
                    String output = process.getProcessOutput().toString();
                    return output.contains(SERVER_STARTED_MARKER)
                            || output.contains(SERVER_FAILED_MARKER)
                            || !process.getErrorOutput().toString().isEmpty()
                            || !process.getProcess().isAlive();
                },
                Duration.ofMinutes(2),
                "Timed out waiting for " + getServerClass().getSimpleName() + " process to start");
        String output = process.getProcessOutput().toString();
        String errorMsg = process.getErrorOutput().toString();
        if (!output.contains(SERVER_STARTED_MARKER)) {
            throw new IllegalStateException(
                    "Server process failed to start. stdout:\n"
                            + output
                            + "\nstderr:\n"
                            + errorMsg);
        }
        boundServerNode = parseBoundServerNode(output, getServerNode());
    }

    private void testConnectionToServer() throws Exception {
        try (NettyClient client =
                new NettyClient(new Configuration(), TestingClientMetricGroup.newInstance())) {
            RpcGateway gateway =
                    GatewayClientProxy.createGatewayProxy(
                            this::clientNode, client, getRpcGatewayClass());
            ApiVersionsResponse response =
                    gateway.apiVersions(
                                    new ApiVersionsRequest()
                                            .setClientSoftwareVersion("1.0.0")
                                            .setClientSoftwareName("test"))
                            .get();
            ApiManager apiManager = new ApiManager(clientNode().serverType());
            assertThat(response.getApiVersionsCount()).isEqualTo(apiManager.enabledApis().size());
        }
    }

    private ServerNode clientNode() {
        return boundServerNode != null ? boundServerNode : getServerNode();
    }

    private static ServerNode parseBoundServerNode(String processOutput, ServerNode template) {
        Matcher matcher = BIND_ENDPOINTS_PATTERN.matcher(processOutput);
        if (!matcher.find()) {
            throw new IllegalStateException(
                    "Could not parse bind endpoints from process output:\n" + processOutput);
        }
        String[] endpoints = matcher.group(1).split(",");
        for (int i = 0; i < endpoints.length; i++) {
            String endpoint = endpoints[i].trim();
            int schemeSep = endpoint.indexOf("://");
            if (schemeSep < 0) {
                continue;
            }
            String hostAndPort = endpoint.substring(schemeSep + 3);
            int colon = hostAndPort.lastIndexOf(':');
            if (colon < 0) {
                continue;
            }
            String host = hostAndPort.substring(0, colon);
            int port = Integer.parseInt(hostAndPort.substring(colon + 1));
            return new ServerNode(
                    template.id(), host, port, template.serverType(), template.rack());
        }
        throw new IllegalStateException("No usable bind endpoint in: " + matcher.group(1));
    }

    private static void generateYamlFile(Path yamlFile, Configuration configuration)
            throws Exception {
        final List<String> configurationLines =
                configuration.toMap().entrySet().stream()
                        .map(entry -> entry.getKey() + ": " + entry.getValue())
                        .collect(Collectors.toList());
        Files.write(yamlFile, configurationLines);
    }
}
