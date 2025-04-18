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

package org.apache.asterix.runtime.aggregates.scalar;

import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractScalarDistinctAggregateDescriptor extends AbstractScalarAggregateDescriptor {

    private static final long serialVersionUID = 1L;
    protected IAType itemType;

    public AbstractScalarDistinctAggregateDescriptor(IFunctionDescriptorFactory aggFuncDescFactory) {
        super(aggFuncDescFactory);
    }

    @Override
    public void setImmutableStates(Object... states) {
        itemType = getItemType((IAType) states[0]);
    }

    @Override
    protected IScalarEvaluator createScalarAggregateEvaluator(IAggregateEvaluator aggEval,
            ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory scanCollectionFactory,
            IEvaluatorContext ctx) throws HyracksDataException {
        return new GenericScalarDistinctAggregateFunction(aggEval, scanCollectionFactory, ctx, sourceLoc, itemType);
    }
}
