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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.row.BinaryArray;
import com.alibaba.fluss.row.BinaryMap;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.DataGetters;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericArray;
import com.alibaba.fluss.row.GenericMap;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.MultisetType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimestampType;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.types.DataTypeChecks.getLength;

/** InternalRowUtils. */
public class InternalRowUtils {

    public static InternalRow copyInternalRow(InternalRow row, RowType rowType) {
        if (row instanceof BinaryRow) {
            return ((BinaryRow) row).copy();
        } else {
            GenericRow ret = new GenericRow(row.getFieldCount());

            for (int i = 0; i < row.getFieldCount(); ++i) {
                DataType fieldType = rowType.getTypeAt(i);
                ret.setField(i, copy(get(row, i, fieldType), fieldType));
            }

            return ret;
        }
    }

    public static InternalArray copyArray(InternalArray from, DataType eleType) {
        if (from instanceof BinaryArray) {
            return ((BinaryArray) from).copy();
        }

        if (!eleType.isNullable()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArray(from.toBooleanArray());
                case TINYINT:
                    return new GenericArray(from.toByteArray());
                case SMALLINT:
                    return new GenericArray(from.toShortArray());
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    return new GenericArray(from.toIntArray());
                case BIGINT:
                    return new GenericArray(from.toLongArray());
                case FLOAT:
                    return new GenericArray(from.toFloatArray());
                case DOUBLE:
                    return new GenericArray(from.toDoubleArray());
            }
        }

        Object[] newArray = new Object[from.size()];

        for (int i = 0; i < newArray.length; ++i) {
            if (!from.isNullAt(i)) {
                newArray[i] = copy(get(from, i, eleType), eleType);
            } else {
                newArray[i] = null;
            }
        }

        return new GenericArray(newArray);
    }

    public static Object copy(Object o, DataType type) {
        if (o instanceof BinaryString) {
            return ((BinaryString) o).copy();
        } else if (o instanceof InternalRow) {
            return copyInternalRow((InternalRow) o, (RowType) type);
        } else if (o instanceof InternalArray) {
            return copyArray((InternalArray) o, ((ArrayType) type).getElementType());
        } else if (o instanceof InternalMap) {
            if (type instanceof MapType) {
                return copyMap(
                        (InternalMap) o,
                        ((MapType) type).getKeyType(),
                        ((MapType) type).getValueType());
            } else {
                return copyMap(
                        (InternalMap) o, ((MultisetType) type).getElementType(), new IntType());
            }
        } else if (o instanceof Decimal) {
            return ((Decimal) o).copy();
        }
        return o;
    }

    private static InternalMap copyMap(InternalMap map, DataType keyType, DataType valueType) {
        if (map instanceof BinaryMap) {
            return ((BinaryMap) map).copy();
        }

        Map<Object, Object> javaMap = new HashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            javaMap.put(
                    copy(get(keys, i, keyType), keyType),
                    copy(get(values, i, valueType), valueType));
        }
        return new GenericMap(javaMap);
    }

    public static Object get(DataGetters dataGetters, int pos, DataType fieldType) {
        if (dataGetters.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return dataGetters.getBoolean(pos);
            case TINYINT:
                return dataGetters.getByte(pos);
            case SMALLINT:
                return dataGetters.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return dataGetters.getInt(pos);
            case BIGINT:
                return dataGetters.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                return dataGetters.getTimestampNtz(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) fieldType;
                return dataGetters.getTimestampLtz(pos, lzTs.getPrecision());
            case FLOAT:
                return dataGetters.getFloat(pos);
            case DOUBLE:
                return dataGetters.getDouble(pos);
            case CHAR:
            case VARCHAR:
            case STRING:
                return dataGetters.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return dataGetters.getDecimal(
                        pos, decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                return dataGetters.getArray(pos);
            case MAP:
            case MULTISET:
                return dataGetters.getMap(pos);
            case ROW:
                return dataGetters.getRow(pos, ((RowType) fieldType).getFieldCount());
            case BINARY:
            case VARBINARY:
                final int binaryLength = getLength(fieldType);
                return dataGetters.getBinary(pos, binaryLength);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }
}
