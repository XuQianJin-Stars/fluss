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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Length-prefixed TCP push-pull transport. A serving instance binds a {@link ServerSocket} and
 * merges inbound views into the shared {@link GossipView}; {@link #clientOnly()} is used by
 * TabletServers that only pull membership.
 */
@Internal
public final class TcpGossipTransport implements GossipTransport {

    private static final Logger LOG = LoggerFactory.getLogger(TcpGossipTransport.class);

    private static final int SO_TIMEOUT_MS = 3_000;

    private final GossipView localView;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServerSocket server;
    private Thread acceptThread;

    /** Serving transport that merges inbound views into {@code localView}. */
    public TcpGossipTransport(GossipView localView) {
        this.localView = localView;
    }

    /** Client-only transport (no listen socket). Used to expand seed lists. */
    public static TcpGossipTransport clientOnly() {
        return new TcpGossipTransport(new GossipView());
    }

    /**
     * Bind {@code port} ({@code 0} = ephemeral). Returns the actual local port.
     *
     * @param port requested listen port
     * @return bound port
     */
    public int bind(int port) throws IOException {
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(port));
        running.set(true);
        acceptThread = new Thread(this::acceptLoop, "fluss-gossip-accept-" + server.getLocalPort());
        acceptThread.setDaemon(true);
        acceptThread.start();
        return server.getLocalPort();
    }

    /** Bound gossip port, or {@code -1} if this transport is client-only. */
    public int localPort() {
        return server == null ? -1 : server.getLocalPort();
    }

    @Override
    public GossipView exchange(GossipMember peer, GossipView outgoing) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(peer.gossipHost(), peer.gossipPort()), SO_TIMEOUT_MS);
            socket.setSoTimeout(SO_TIMEOUT_MS);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            GossipCodec.write(out, outgoing);
            out.flush();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            return GossipCodec.read(in, System.currentTimeMillis());
        }
    }

    /** Closes the listen socket if this transport is serving. */
    @Override
    public void close() throws IOException {
        running.set(false);
        if (server != null) {
            try {
                server.close();
            } catch (IOException e) {
                LOG.debug("Error closing gossip server socket", e);
            }
        }
        if (acceptThread != null) {
            acceptThread.interrupt();
        }
    }

    private void acceptLoop() {
        while (running.get() && server != null && !server.isClosed()) {
            try {
                Socket socket = server.accept();
                try {
                    handle(socket);
                } finally {
                    try {
                        socket.close();
                    } catch (IOException ignored) {
                        // close quietly
                    }
                }
            } catch (SocketException e) {
                if (running.get()) {
                    LOG.debug("Gossip accept socket closed", e);
                }
            } catch (IOException e) {
                if (running.get()) {
                    LOG.debug("Gossip accept failed", e);
                }
            }
        }
    }

    private void handle(Socket socket) throws IOException {
        socket.setSoTimeout(SO_TIMEOUT_MS);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        GossipView remote = GossipCodec.read(in, System.currentTimeMillis());
        localView.merge(remote, System.currentTimeMillis());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        GossipCodec.write(out, localView.snapshot());
        out.flush();
    }
}
