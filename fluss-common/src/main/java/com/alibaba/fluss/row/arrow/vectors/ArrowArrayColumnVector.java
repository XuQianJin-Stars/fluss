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

import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.columnar.ArrayColumnVector;
import com.alibaba.fluss.row.columnar.ColumnVector;
import com.alibaba.fluss.row.columnar.ColumnarArray;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.ArrowUtils;

/** ArrowArrayColumnVector is a wrapper class for Arrow ListVector. */
public class ArrowArrayColumnVector implements ArrayColumnVector {
    private boolean inited = false;
    private FieldVector vector;
    private final DataType elementType;
    private ColumnVector columnVector;

    public ArrowArrayColumnVector(FieldVector vector, DataType elementType) {
        this.vector = vector;
        this.elementType = elementType;
    }

    private void init() {
        if (!inited) {
            FieldVector child = ((ListVector) vector).getDataVector();
            this.columnVector = ArrowUtils.createArrowColumnVector(child, elementType);
            inited = true;
        }
    }

    @Override
    public InternalArray getArray(int index) {
        init();
        ListVector listVector = (ListVector) vector;
        int start = listVector.getElementStartIndex(index);
        int end = listVector.getElementEndIndex(index);
        return new ColumnarArray(columnVector, start, end - start);
    }

    @Override
    public ColumnVector getColumnVector() {
        init();
        return columnVector;
    }

    @Override
    public boolean isNullAt(int index) {
        return vector.isNull(index);
    }
}
