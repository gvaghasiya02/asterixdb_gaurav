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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

@MissingNullInOutFunction
public class StringLowerCaseDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = StringLowerCaseDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractUnaryStringStringEval(ctx, args[0], StringLowerCaseDescriptor.this.getIdentifier(),
                        sourceLoc) {

                    @Override
                    protected void process(UTF8StringPointable inputString, IPointable resultPointable)
                            throws IOException {
                        inputString.lowercase(resultBuilder, resultArray);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_LOWERCASE;
    }

}
