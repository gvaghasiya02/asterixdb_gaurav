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
package org.apache.hyracks.dataflow.std.hashmap;

import java.util.Iterator;

import org.apache.hyracks.dataflow.std.hashmap.entry.IUnsafeMapResultAppender;
import org.apache.hyracks.unsafe.BytesToBytesMap;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.unsafe.entry.IEntryComparator;
import org.apache.spark.unsafe.memory.MemoryAllocator;

import com.google.common.annotations.VisibleForTesting;

public final class UnsafeHashAggregator extends AbstractUnsafeHashAggregator {
    private final BytesToBytesMap map;
    private final IEntry aggregate;
    private final long budget;

    public UnsafeHashAggregator(IUnsafeAggregator aggregator, IUnsafeMapResultAppender appender,
            IEntryComparator keyComparator, long budget) {
        super(aggregator, appender);
        map = new BytesToBytesMap(MemoryAllocator.HEAP, budget, 1024, keyComparator);
        aggregate = aggregator.createValueEntry();
        this.budget = budget;
    }

    @Override
    public boolean aggregate(IEntry key, IEntry value) {
        Location location = map.lookup(key);
        if (location.isDefined()) {
            aggregate.getValue(location);
            aggregator.aggregate(aggregate, value);
            aggregate.setValue(location);
            return true;
        }
        //New key
        aggregator.initAggregateValue(aggregate, value);
        return location.append(key, aggregate);
    }

    @Override
    public void append(AILResultWriter resultWriter) {
        Iterator<Location> iterator = map.iterator();
        while (iterator.hasNext()) {
            Location location = iterator.next();
            appender.appendKey(resultWriter, location);
            appender.appendValue(resultWriter, location);
            resultWriter.flush();
        }
        map.reset();
    }

    @VisibleForTesting
    public int size() {
        return map.numKeys();
    }

    public int numValues() {
        return map.numValues();
    }

    @VisibleForTesting
    public Iterator<Location> sortedIterator() {
        map.sort();
        return map.sortedIterator();
    }

    public Iterator<Location> aIterator() {
        return map.iterator();
    }

    public void reset() {
        map.reset();
    }

    public long getSizeofHashEntries() {
        return map.getTotalSizeofHashEntries();
    }

    public long getTotalMemoryConsumption() {
        return map.getTotalMemoryConsumption();
    }

    public int getNumDataPages() {
        return map.getNumDataPages();
    }
}
