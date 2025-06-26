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

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.TimestampType;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

/** AssertJ assert for {@link InternalArray}. */
public class InternalArrayAssert extends AbstractAssert<InternalArrayAssert, InternalArray> {

    private DataType elementType;

    InternalArrayAssert(InternalArray actual) {
        super(actual, InternalArrayAssert.class);
    }

    public static InternalArrayAssert assertThatArray(InternalArray actual) {
        return new InternalArrayAssert(actual);
    }

    public InternalArrayAssert withElementType(DataType elementType) {
        this.elementType = elementType;
        return this;
    }

    public InternalArrayAssert isEqualTo(InternalArray expected) {
        assert elementType != null;
        assertThat(actual.size()).isEqualTo(expected.size());
        switch (elementType.getTypeRoot()) {
            case CHAR:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getChar(i, expected.getChar(i, 0).numChars()))
                            .isEqualTo(expected.getChar(i, 0));
                }
                break;
            case VARCHAR:
            case STRING:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getString(i)).isEqualTo(expected.getString(i));
                }
                break;
            case BOOLEAN:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getBoolean(i)).isEqualTo(expected.getBoolean(i));
                }
                break;
            case BINARY:
            case VARBINARY:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getBinary(i)).isEqualTo(expected.getBinary(i));
                }
                break;
            case BYTES:
            case TINYINT:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getByte(i)).isEqualTo(expected.getByte(i));
                }
                break;
            case DECIMAL:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getDecimal(i, 0, 0)).isEqualTo(expected.getDecimal(i, 0, 0));
                }
                break;
            case SMALLINT:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getShort(i)).isEqualTo(expected.getShort(i));
                }
                break;
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getInt(i)).isEqualTo(expected.getInt(i));
                }
                break;
            case BIGINT:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getLong(i)).isEqualTo(expected.getLong(i));
                }
                break;
            case FLOAT:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getFloat(i)).isEqualTo(expected.getFloat(i));
                }
                break;
            case DOUBLE:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getDouble(i)).isEqualTo(expected.getDouble(i));
                }
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) elementType;
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getTimestampNtz(i, timestampType.getPrecision()))
                            .isEqualTo(expected.getTimestampNtz(i, timestampType.getPrecision()));
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) elementType;
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getTimestampLtz(i, localZonedTimestampType.getPrecision()))
                            .isEqualTo(
                                    expected.getTimestampLtz(
                                            i, localZonedTimestampType.getPrecision()));
                }
                break;
            case ARRAY:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getArray(i)).isEqualTo(expected.getArray(i));
                }
                break;
            case MAP:
                for (int i = 0; i < actual.size(); i++) {
                    assertThat(actual.getMap(i)).isEqualTo(expected.getMap(i));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported element type: " + elementType);
        }
        return this;
    }
}
