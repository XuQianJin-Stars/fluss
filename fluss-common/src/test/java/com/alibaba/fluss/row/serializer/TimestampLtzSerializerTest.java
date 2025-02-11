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

import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.utils.DateTimeUtils;
import com.alibaba.fluss.utils.Pair;

import java.util.Arrays;
import java.util.List;

/** Test for {@link TimestampNtzSerializer}. */
public abstract class TimestampLtzSerializerTest extends SerializerTestBase<TimestampLtz> {

    @Override
    protected Serializer<TimestampLtz> createSerializer() {
        return new TimestampLtzSerializer(getPrecision());
    }

    @Override
    protected boolean deepEquals(TimestampLtz t1, TimestampLtz t2) {
        return t1.equals(t2);
    }

    @Override
    protected TimestampLtz[] getTestData() {
        return new TimestampLtz[] {
            TimestampLtz.fromEpochMillis(1),
            TimestampLtz.fromEpochMillis(2),
            TimestampLtz.fromEpochMillis(3),
            TimestampLtz.fromEpochMillis(4)
        };
    }

    protected abstract int getPrecision();

    static final class TimestampNtzSerializer0Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }

        @Override
        protected List<Pair<TimestampLtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:00", getPrecision()),
                            "2019-01-01 00:00:00"),
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:01", getPrecision()),
                            "2019-01-01 00:00:01"));
        }
    }

    static final class TimestampNtzSerializer3Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }

        @Override
        protected List<Pair<TimestampLtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:00.000", getPrecision()),
                            "2019-01-01 00:00:00.000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:00.001", getPrecision()),
                            "2019-01-01 00:00:00.001"));
        }
    }

    static final class TimestampNtzSerializer6Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }

        @Override
        protected List<Pair<TimestampLtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:00.000000", getPrecision()),
                            "2019-01-01 00:00:00.000000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:00.000001", getPrecision()),
                            "2019-01-01 00:00:00.000001"));
        }
    }

    static final class TimestampNtzSerializer8Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }

        @Override
        protected List<Pair<TimestampLtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:00.00000000", getPrecision()),
                            "2019-01-01 00:00:00.00000000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampLtzData(
                                    "2019-01-01 00:00:00.00000001", getPrecision()),
                            "2019-01-01 00:00:00.00000001"));
        }
    }
}
