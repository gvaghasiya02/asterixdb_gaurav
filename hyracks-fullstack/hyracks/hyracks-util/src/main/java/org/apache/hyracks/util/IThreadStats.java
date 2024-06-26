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

public interface IThreadStats {

    /**
     * Indicates that this thread attempted to pin a page
     */
    void pagePinned();

    /**
     * Gets the count of attempts made by this thread to pin a page
     *
     * @return the pinned pages count
     */
    long getPinnedPagesCount();

    /**
     * Indicates that this thread caused a cold read from disk
     */
    void coldRead();

    /**
     * Gets the count of pages read in from disk
     *
     * @return the cold read count
     */
    long getColdReadCount();

    /**
     * Indicates that this thread made a cloud request to object storage
     */
    void cloudReadRequest();

    /**
     * Gets the count of cloud request to object storage
     *
     * @return the cloud request count
     */
    long getCloudReadRequestCount();

    /**
     * Indicates a page is read from the cloud
     */
    void cloudPageRead();

    /**
     * @return the count of pages read from the cloud
     */
    long getCloudPageReadCount();

    /**
     * Indicates the page is persistent in the disk,
     * after fetching from cloud.
     */
    void cloudPagePersist();

    /**
     * @return the count of fetched page is persisted in the disk.
     */
    long getCloudPagePersistCount();
}
