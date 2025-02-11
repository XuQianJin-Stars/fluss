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

package com.alibaba.fluss.row.serializer;

import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.MultisetType;

import static com.alibaba.fluss.types.DataTypeChecks.getFieldTypes;
import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;
import static com.alibaba.fluss.types.DataTypeChecks.getScale;

/** {@link Serializer} of {@link DataType} for internal data structures. */
public final class InternalSerializers {

    /** Creates a {@link Serializer} for internal data structures of the given {@link DataType}. */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> create(DataType type) {
        return (Serializer<T>) createInternal(type);
    }

    private static Serializer<?> createInternal(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case STRING:
                return BinaryStringSerializer.INSTANCE;
            case BOOLEAN:
                return BooleanSerializer.INSTANCE;
            case BINARY:
            case VARBINARY:
                return BinarySerializer.INSTANCE;
            case DECIMAL:
                return new DecimalSerializer(getPrecision(type), getScale(type));
            case TINYINT:
                return ByteSerializer.INSTANCE;
            case SMALLINT:
                return ShortSerializer.INSTANCE;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return IntSerializer.INSTANCE;
            case BIGINT:
                return LongSerializer.INSTANCE;
            case FLOAT:
                return FloatSerializer.INSTANCE;
            case DOUBLE:
                return DoubleSerializer.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampNtzSerializer(getPrecision(type));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampLtzSerializer(getPrecision(type));
            case ARRAY:
                return new InternalArraySerializer(((ArrayType) type).getElementType());
            case MULTISET:
                return new InternalMapSerializer(
                        ((MultisetType) type).getElementType(), new IntType(false));
            case MAP:
                MapType mapType = (MapType) type;
                return new InternalMapSerializer(mapType.getKeyType(), mapType.getValueType());
            case ROW:
                return new InternalRowSerializer(getFieldTypes(type).toArray(new DataType[0]));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type '" + type + "' to get internal serializer");
        }
    }

    private InternalSerializers() {
        // no instantiation
    }
}
