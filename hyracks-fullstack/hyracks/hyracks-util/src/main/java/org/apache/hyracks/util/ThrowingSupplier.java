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
package org.apache.hyracks.util;

import java.util.function.Supplier;

import com.google.common.util.concurrent.UncheckedExecutionException;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;

    @SuppressWarnings("Duplicates")
    static <T> Supplier<T> asUnchecked(ThrowingSupplier<T> supplier) {
        return () -> {
            try {
                return supplier.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException(e);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        };
    }

}
