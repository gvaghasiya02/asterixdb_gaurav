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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.input.record.ValueReferenceRecord;
import org.apache.asterix.external.input.record.reader.hdfs.AbstractHDFSRecordReader;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * Apache Parquet record reader.
 * The reader returns records in ADM format.
 */
public class ParquetFileRecordReader<V extends IValueReference> extends AbstractHDFSRecordReader<Void, V> {
    private final IWarningCollector warningCollector;

    public ParquetFileRecordReader(boolean[] read, InputSplit[] inputSplits, String[] readSchedule, String nodeName,
            JobConf conf, IExternalDataRuntimeContext context, UserGroupInformation ugi) {
        super(read, inputSplits, readSchedule, nodeName, new ValueReferenceRecord<>(), conf, ugi);
        this.warningCollector = context.getTaskContext().getWarningCollector();
        ((MapredParquetInputFormat) inputFormat).setValueEmbedder(context.getValueEmbedder());
    }

    @Override
    protected boolean onNextInputSplit() throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {
        super.close();
        //Issue warning if any was reported
        HDFSUtils.issueWarnings(warningCollector, conf);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setRecordReader(int splitIndex) throws IOException {
        try {
            ParquetRecordReaderWrapper readerWrapper = ugi == null ? getReader(splitIndex)
                    : ugi.doAs((PrivilegedExceptionAction<ParquetRecordReaderWrapper>) () -> getReader(splitIndex));
            reader = (RecordReader<Void, V>) readerWrapper;
        } catch (AsterixParquetRuntimeException e) {
            throw e.getHyracksDataException();
        } catch (InterruptedException e) {
            throw HyracksDataException.create(e);
        }
        if (value == null) {
            value = reader.createValue();
        }
    }

    private ParquetRecordReaderWrapper getReader(int splitIndex) throws IOException {
        return (ParquetRecordReaderWrapper) inputFormat.getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL);
    }
}
