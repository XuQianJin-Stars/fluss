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

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.columnar.ColumnVector;
import com.alibaba.fluss.row.columnar.ColumnarRow;
import com.alibaba.fluss.row.columnar.RowColumnVector;
import com.alibaba.fluss.row.columnar.VectorizedColumnBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.ArrowUtils;

import java.util.List;

/** ArrowRowColumnVector is a wrapper class for Arrow RowVector. */
public class ArrowRowColumnVector implements RowColumnVector {
    private boolean inited = false;
    private final FieldVector vector;
    private final RowType rowType;
    private VectorizedColumnBatch vectorizedColumnBatch;

    public ArrowRowColumnVector(FieldVector vector, RowType rowType) {
        this.vector = vector;
        this.rowType = rowType;
    }

    private void init() {
        if (!inited) {
            List<FieldVector> children = ((StructVector) vector).getChildrenFromFields();
            ColumnVector[] vectors = new ColumnVector[children.size()];
            for (int i = 0; i < children.size(); i++) {
                vectors[i] =
                        ArrowUtils.createArrowColumnVector(children.get(i), rowType.getTypeAt(i));
            }
            this.vectorizedColumnBatch = new VectorizedColumnBatch(vectors);
            inited = true;
        }
    }

    @Override
    public InternalRow getRow(int i) {
        init();
        return new ColumnarRow(vectorizedColumnBatch, i);
    }

    @Override
    public VectorizedColumnBatch getBatch() {
        init();
        return vectorizedColumnBatch;
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }
}
