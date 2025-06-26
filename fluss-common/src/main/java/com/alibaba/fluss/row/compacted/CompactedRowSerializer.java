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

package com.alibaba.fluss.row.compacted;

import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.serializer.InternalRowSerializer;
import com.alibaba.fluss.row.serializer.InternalSerializers;
import com.alibaba.fluss.row.serializer.Serializer;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;

import static com.alibaba.fluss.types.DataTypeChecks.getFieldTypes;

/** CompactedRowSerializer. */
public class CompactedRowSerializer extends InternalRowSerializer {
    /** Creates a {@link Serializer} for internal data structures of the given {@link DataType}. */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> create(DataType type) {
        return (Serializer<T>) createInternal(type);
    }

    private static Serializer<?> createInternal(DataType type) {
        if (type.getTypeRoot() == DataTypeRoot.ROW) {
            return new InternalRowSerializer(
                    getFieldTypes(type).toArray(new DataType[0]), KvFormat.COMPACTED);
        }
        return InternalSerializers.create(type);
    }

    private CompactedRowSerializer() {
        // no instantiation
        super(new DataType[0], KvFormat.COMPACTED);
    }
}
