/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class LSMBTreeBatchPointSearchOperatorNodePushable extends BTreeSearchOperatorNodePushable {

    private final int[] keyFields;

    public LSMBTreeBatchPointSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, int[] minFilterKeyFields, int[] maxFilterKeyFields,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, ITupleProjectorFactory tupleProjectorFactory,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterKeyFields, maxFilterKeyFields, indexHelperFactory, retainInput, retainMissing,
                missingWriterFactory, searchCallbackFactory, false, null, tupleFilterFactory, outputLimit, false, null,
                null, tupleProjectorFactory, tuplePartitionerFactory, partitionsMap);
        this.keyFields = lowKeyFields;
    }

    @Override
    protected IIndexCursor createCursor(IIndex idx, IIndexAccessor idxAccessor) throws HyracksDataException {
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) idxAccessor;
        return ((LSMBTree) idx).createBatchPointSearchCursor(lsmAccessor.getOpContext());
    }

    @Override
    protected ISearchPredicate createSearchPredicate(IIndex index) {
        ITreeIndex treeIndex = (ITreeIndex) index;
        lowKeySearchCmp =
                highKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), lowKey);
        return new BatchPredicate(accessor, lowKeySearchCmp, keyFields, minFilterFieldIndexes, maxFilterFieldIndexes);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        if (accessor.getTupleCount() > 0) {
            BatchPredicate batchPred = (BatchPredicate) searchPred;
            for (int p = 0; p < partitions.length; p++) {
                batchPred.reset(accessor);
                try {
                    indexAccessors[p].search(cursors[p], batchPred);
                    writeSearchResults(cursors[p]);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                } finally {
                    cursors[p].close();
                }
            }
        }
    }

    protected void writeSearchResults(IIndexCursor cursor) throws IOException {
        long matchingTupleCount = 0;
        LSMBTreeBatchPointSearchCursor batchCursor = (LSMBTreeBatchPointSearchCursor) cursor;
        int tupleIndex = 0;
        while (cursor.hasNext()) {
            cursor.next();
            matchingTupleCount++;
            ITupleReference tuple = cursor.getTuple();
            tb.reset();

            if (retainInput && retainMissing) {
                appendMissingTuple(tupleIndex, batchCursor.getKeyIndex());
            }

            tupleIndex = batchCursor.getKeyIndex();

            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            ITupleReference projectedTuple = writeTupleToOutput(tuple);
            if (tupleFilter != null) {
                referenceFilterTuple.reset(projectedTuple);
                if (!tupleFilter.accept(referenceFilterTuple)) {
                    continue;
                }
            }
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (outputLimit >= 0 && ++outputCount >= outputLimit) {
                finished = true;
                break;
            }
        }

        if (matchingTupleCount == 0 && retainInput && retainMissing) {
            int end = accessor.getTupleCount();
            appendMissingTuple(0, end);
        }
        stats.getInputTupleCounter().update(matchingTupleCount);
    }

    private void appendMissingTuple(int start, int end) throws HyracksDataException {
        for (int i = start; i < end; i++) {
            FrameUtils.appendConcatToWriter(writer, appender, accessor, i, nonMatchTupleBuild.getFieldEndOffsets(),
                    nonMatchTupleBuild.getByteArray(), 0, nonMatchTupleBuild.getSize());
        }
    }

}
