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

package com.alibaba.fluss.row.arrow.vectors;

import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.row.columnar.ColumnVector;
import com.alibaba.fluss.row.columnar.ColumnarMap;
import com.alibaba.fluss.row.columnar.MapColumnVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.MapVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.ArrowUtils;

/** ArrowArrayColumnVector is a wrapper class for Arrow MapVector. */
public class ArrowMapColumnVector implements MapColumnVector {
    private boolean inited = false;
    private final FieldVector vector;
    private final DataType keyType;
    private final DataType valueType;
    private ColumnVector keyColumnVector;
    private ColumnVector valueColumnVector;

    public ArrowMapColumnVector(FieldVector vector, DataType keyType, DataType valueType) {
        this.vector = vector;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    private void init() {
        if (!inited) {
            FieldVector mapVector = ((MapVector) vector).getDataVector();
            StructVector structVector = (StructVector) mapVector;
            FieldVector keyVector = structVector.getChildrenFromFields().get(0);
            FieldVector valueVector = structVector.getChildrenFromFields().get(1);
            this.keyColumnVector = ArrowUtils.createArrowColumnVector(keyVector, keyType);
            this.valueColumnVector = ArrowUtils.createArrowColumnVector(valueVector, valueType);
            inited = true;
        }
    }

    @Override
    public InternalMap getMap(int i) {
        init();
        MapVector mapVector = (MapVector) vector;
        int start = mapVector.getElementStartIndex(i);
        int end = mapVector.getElementEndIndex(i);

        return new ColumnarMap(keyColumnVector, valueColumnVector, start, end - start);
    }

    @Override
    public ColumnVector getKeyColumnVector() {
        init();
        return keyColumnVector;
    }

    @Override
    public ColumnVector getValueColumnVector() {
        init();
        return valueColumnVector;
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    @Override
    public ColumnVector[] getChildren() {
        return new ColumnVector[] {keyColumnVector, valueColumnVector};
    }
}
