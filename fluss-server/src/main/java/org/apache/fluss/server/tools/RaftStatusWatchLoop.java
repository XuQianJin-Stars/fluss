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

package org.apache.fluss.server.tools;

import org.apache.fluss.annotation.Internal;

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * Polling helper for {@code fluss raft status --watch} (design §7.4.5). Prints one status snapshot
 * per interval until {@code stop} is true or the current thread is interrupted.
 */
@Internal
public final class RaftStatusWatchLoop {

    /** Default poll interval when {@code --interval} is omitted. */
    public static final long DEFAULT_INTERVAL_MS = 2_000L;

    private RaftStatusWatchLoop() {}

    /** Sleeps {@code millis} (extracted so tests can skip real waits). */
    public interface Sleeper {
        /**
         * Block for {@code millis}.
         *
         * @param millis sleep duration
         */
        void sleep(long millis) throws InterruptedException;
    }

    /**
     * Repeatedly invoke {@code tick} until {@code stop} returns true.
     *
     * @param intervalMs pause between ticks (must be {@code >= 0})
     * @param stop cooperative cancellation
     * @param sleeper pause implementation
     * @param tick one status fetch+print; return value is the process exit code
     * @return last tick's exit code (or {@code 1} if no tick ran)
     */
    public static int run(long intervalMs, BooleanSupplier stop, Sleeper sleeper, IntSupplier tick)
            throws InterruptedException {
        long pause = intervalMs < 0L ? DEFAULT_INTERVAL_MS : intervalMs;
        int last = 1;
        boolean first = true;
        while (!stop.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
            if (!first) {
                sleeper.sleep(pause);
                if (stop.getAsBoolean() || Thread.currentThread().isInterrupted()) {
                    break;
                }
            }
            first = false;
            last = tick.getAsInt();
        }
        return last;
    }
}
