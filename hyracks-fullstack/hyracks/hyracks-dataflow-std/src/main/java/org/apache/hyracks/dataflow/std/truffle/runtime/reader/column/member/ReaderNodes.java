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
package org.apache.hyracks.dataflow.std.truffle.runtime.reader.column.member;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.GenerateUncached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.Node;
import org.apache.asterix.codegen.asterix.column.reader.*;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.array.storage.*;

public class ReaderNodes {
    @GenerateUncached
    abstract static class GetValueNode extends Node {
        public abstract Object executeGeneric(AbstractTypedColumnReader reader);

        public abstract boolean executeBoolean(AbstractTypedColumnReader reader);

        public abstract long executeLong(AbstractTypedColumnReader reader);

        public abstract double executeDouble(AbstractTypedColumnReader reader);

        @Specialization
        @TruffleBoundary
        boolean getBoolean(BooleanColumnReader reader) {
            return reader.getBoolean();
        }

        @Specialization
        long getLong(LongColumnReader reader) {
            return reader.getLong();
        }

        @Specialization
        double getDouble(DoubleColumnReader reader) {
            return reader.getDouble();
        }

        @Specialization
        AILStringRuntime getString(StringColumnReader reader) {
            return reader.getString();
        }
    }

    @GenerateUncached
    abstract static class ToArrayNode extends Node {
        public abstract AbstractArrayStorage executeGeneric(AbstractTypedColumnReader reader);

        @Specialization
        BooleanArrayStorage getBoolean(BooleanColumnReader reader) {
            return new BooleanArrayStorage(reader);
        }

        @Specialization
        LongArrayStorage getLong(LongColumnReader reader) {
            return new LongArrayStorage(reader);
        }

        @Specialization
        DoubleArrayStorage getDouble(DoubleColumnReader reader) {
            return new DoubleArrayStorage(reader);
        }

        @Specialization
        StringArrayStorage getString(StringColumnReader reader) {
            return new StringArrayStorage(reader);
        }
    }
}
