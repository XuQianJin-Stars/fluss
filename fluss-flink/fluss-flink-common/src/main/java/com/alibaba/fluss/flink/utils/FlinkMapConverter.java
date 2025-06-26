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
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.utils.InternalRowUtils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter.createInternalConverter;

/** MapData Converter. */
public class FlinkMapConverter implements MapData {

    private final MapData mapData;

    FlinkMapConverter(DataType eleType, Object flussField) {
        this.mapData =
                copyMap(
                        (InternalMap) flussField,
                        ((MapType) eleType).getKeyType(),
                        ((MapType) eleType).getValueType());
    }

    private MapData copyMap(InternalMap map, DataType keyType, DataType valueType) {
        FlussDeserializationConverter keyConverter = createInternalConverter(keyType);
        FlussDeserializationConverter valueConverter = createInternalConverter(valueType);
        Map<Object, Object> javaMap = new HashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            Object key =
                    keyConverter.deserialize(
                            InternalRowUtils.copy(InternalRowUtils.get(keys, i, keyType), keyType));
            Object value =
                    InternalRowUtils.get(values, i, valueType) == null
                            ? null
                            : valueConverter.deserialize(
                                    InternalRowUtils.copy(
                                            InternalRowUtils.get(values, i, valueType), keyType));
            javaMap.put(key, value);
        }
        return new GenericMapData(javaMap);
    }

    @Override
    public int size() {
        return mapData.size();
    }

    @Override
    public ArrayData keyArray() {
        return mapData.keyArray();
    }

    @Override
    public ArrayData valueArray() {
        return mapData.valueArray();
    }

    public MapData getMapData() {
        return mapData;
    }

    public static MapData deserialize(DataType flussDataType, Object flussField) {
        return new FlinkMapConverter(flussDataType, flussField).getMapData();
    }
}
