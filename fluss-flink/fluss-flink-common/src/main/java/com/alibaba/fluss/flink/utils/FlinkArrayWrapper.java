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

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import static com.alibaba.fluss.flink.utils.FlinkRowWrapper.fromFlinkDecimal;
import static com.alibaba.fluss.flink.utils.FlinkRowWrapper.fromFlinkString;
import static com.alibaba.fluss.flink.utils.FlinkRowWrapper.fromFlinkTimestampLtz;
import static com.alibaba.fluss.flink.utils.FlinkRowWrapper.fromFlinkTimestampNtz;

/** Wrapper for Flink's internal array data structure. */
public class FlinkArrayWrapper implements InternalArray {

    private final org.apache.flink.table.data.ArrayData array;

    public FlinkArrayWrapper(org.apache.flink.table.data.ArrayData array) {
        this.array = array;
    }

    @Override
    public int size() {
        return array.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return array.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return array.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return array.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return array.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return array.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return array.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return array.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return array.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return getString(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return fromFlinkString(array.getString(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(array.getDecimal(pos, precision, scale));
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return fromFlinkTimestampNtz(array.getTimestamp(pos, precision));
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return fromFlinkTimestampLtz(array.getTimestamp(pos, precision));
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return array.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return array.getBinary(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return array.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new FlinkArrayWrapper(array.getArray(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new FlinkMapWrapper(array.getMap(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new FlinkRowWrapper(array.getRow(pos, numFields));
    }

    @Override
    public boolean[] toBooleanArray() {
        return array.toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return array.toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return array.toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return array.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return array.toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return array.toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return array.toDoubleArray();
    }
}
