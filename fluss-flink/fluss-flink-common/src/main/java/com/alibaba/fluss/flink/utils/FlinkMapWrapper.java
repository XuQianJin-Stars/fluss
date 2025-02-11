/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;

/** Wrapper for Flink MapData. */
public class FlinkMapWrapper implements InternalMap {
    private final org.apache.flink.table.data.MapData map;

    public FlinkMapWrapper(org.apache.flink.table.data.MapData map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public InternalArray keyArray() {
        return new FlinkArrayWrapper(map.keyArray());
    }

    @Override
    public InternalArray valueArray() {
        return new FlinkArrayWrapper(map.valueArray());
    }
}
