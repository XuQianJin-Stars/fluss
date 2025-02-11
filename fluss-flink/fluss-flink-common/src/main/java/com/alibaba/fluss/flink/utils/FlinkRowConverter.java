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

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.InternalRowUtils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import static com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter.createInternalConverter;

/** FlinkRowConverter. */
public class FlinkRowConverter implements RowData {

    private final RowData rowData;

    FlinkRowConverter(DataType eleType, Object flussField) {
        this.rowData = copyInternalRow((InternalRow) flussField, (RowType) eleType);
    }

    public RowData copyInternalRow(InternalRow row, RowType rowType) {
        GenericRowData ret = new GenericRowData(row.getFieldCount());

        for (int i = 0; i < row.getFieldCount(); ++i) {
            DataType fieldType = rowType.getTypeAt(i);
            FlussDeserializationConverter converter = createInternalConverter(fieldType);
            ret.setField(
                    i,
                    converter.deserialize(
                            InternalRowUtils.copy(
                                    InternalRowUtils.get(row, i, fieldType), fieldType)));
        }

        return ret;
    }

    @Override
    public int getArity() {
        return rowData.getArity();
    }

    @Override
    public RowKind getRowKind() {
        return rowData.getRowKind();
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        rowData.setRowKind(rowKind);
    }

    @Override
    public boolean isNullAt(int i) {
        return rowData.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return rowData.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return rowData.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return rowData.getShort(i);
    }

    @Override
    public int getInt(int i) {
        return rowData.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return rowData.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return rowData.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return rowData.getDouble(i);
    }

    @Override
    public StringData getString(int i) {
        return rowData.getString(i);
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        return rowData.getDecimal(i, i1, i2);
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        return rowData.getTimestamp(i, i1);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        return rowData.getRawValue(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return rowData.getBinary(i);
    }

    @Override
    public ArrayData getArray(int i) {
        return rowData.getArray(i);
    }

    @Override
    public MapData getMap(int i) {
        return rowData.getMap(i);
    }

    @Override
    public RowData getRow(int i, int i1) {
        return rowData.getRow(i, i1);
    }

    public RowData getRowData() {
        return rowData;
    }

    public static RowData deserialize(DataType flussDataType, Object flussField) {
        return new FlinkRowConverter(flussDataType, flussField).getRowData();
    }
}
