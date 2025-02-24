/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.source.split;

/** The state of {@link LogSplit}. */
public class LogSplitState extends SourceSplitState {

    private long offset;

    public LogSplitState(LogSplit split) {
        super(split);
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public LogSplit toSourceSplit() {
        final LogSplit logSplit = (LogSplit) split;
        return new LogSplit(
                logSplit.tablePath, logSplit.tableBucket, logSplit.getPartitionName(), offset);
    }
}
