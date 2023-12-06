## Optimization of Group By Queries

Optimizing group by queries in Apache AsterixDB, a scalable Big data management system for semi-structured data,
using hash tables and memory-based sort to enhance query performance and reduce execution time by 6x.

### Contributors:

Gaurav Vaghasiya

**Guided By**: Dr. Shiva Jahangiri

### Detailed Description:
1. **Local Aggregation Phase**:
- In the local aggregation phase, data partitions process their respective subsets of the data.
- For each partition, a local hash map is used to perform group-wise aggregation. This local hash map stores partial aggregation results for each group within the partition.
2. **Hash Partition Exchange (Local to Global)**:
- When the local hash map becomes full (indicating that it can't hold more aggregated data), the partition sends its local hash map, which contains partial aggregation results, to the global aggregation phase.
- Hash partition exchange ensures that the data sent to the global aggregation phase is properly partitioned based on the group keys. This partitioning is done using a hash function so that data with the same group key ends up in the same global aggregator.
3. **Global Aggregation and Finalization (Including Sort and Merge)**:
- In the global aggregation phase, multiple global aggregators (corresponding to different group keys) receive data from local partitions.
- Each global aggregator aggregates the data it receives from local partitions to compute the final global aggregation result for its specific group.
- When a global aggregator's memory is about to reach its limit, it performs the following steps:
  - Sort the aggregated data entries based on the group key.
  - Write the sorted data entries to disk as a sorted run.
- This process continues until all global aggregators have processed their data, resulting in several sorted runs for each group.
- Finally, the sorted runs are merged to compute the final aggregate result for each group. This merge process combines the sorted runs efficiently, considering the group keys, and results in the global aggregate values.


### Milestones:

**1. Project Initiation** :

**Status**:Completed

- Create a project plan and schedule.

**2. Local Aggregation**

**Status**:Completed

- Develop local aggregation logic.
- Test and optimize local aggregation.

**3. Hash Partition Exchange**

- Pass the local Operator data to the global Operator

**4. Global Aggregation and Finalization**
- Develop global aggregation logic.
- Implement sorting and merging of hash tables.
- Test and optimize global aggregation.

**5. Integration and Testing**
- Integrate components and perform end-to-end testing.
- Address integration issues.


## About AsterixDB:





<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->
<a href="http://asterixdb.apache.org"><img src="http://asterixdb.apache.org/img/asterixdb_tm.png" height=100></img></a>

## What is AsterixDB?

AsterixDB is a BDMS (Big Data Management System) with a rich feature set that sets it apart from other Big Data platforms.  Its feature set makes it well-suited to modern needs such as web data warehousing and social data storage and analysis. AsterixDB has:

- __Data model__<br/>
  A semistructured NoSQL style data model ([ADM](https://ci.apache.org/projects/asterixdb/datamodel.html)) resulting from
  extending JSON with object database ideas

- __Query languages__<br/>
  An expressive and declarative query language ([SQL++](http://asterixdb.apache.org/docs/0.9.7/sqlpp/manual.html) that supports a broad range of queries and analysis over semistructured data

- __Scalability__<br/>
  A parallel runtime query execution engine, Apache Hyracks, that has been scale-tested on up to 1000+ cores and 500+ disks

- __Native storage__<br/>
  Partitioned LSM-based data storage and indexing to support efficient ingestion and management of semistructured data

- __External storage__<br/>
  Support for query access to externally stored data (e.g., data in HDFS) as well as to data stored natively by AsterixDB

- __Data types__<br/>
  A rich set of primitive data types, including spatial and temporal data in addition to integer, floating point, and textual data

- __Indexing__<br/>
  Secondary indexing options that include B+ trees, R trees, and inverted keyword (exact and fuzzy) index types

- __Transactions__<br/>
  Basic transactional (concurrency and recovery) capabilities akin to those of a NoSQL store

Learn more about AsterixDB at its [website](http://asterixdb.apache.org).


## Build from source

To build AsterixDB from source, you should have a platform with the following:

* A Unix-ish environment (Linux, OS X, will all do).
* git
* Maven 3.3.9 or newer.
* JDK 11 or newer.
* Python 3.6+ with pip and venv

Instructions for building the master:

* Checkout AsterixDB master:

        $git clone https://github.com/apache/asterixdb.git

* Build AsterixDB master:

        $cd asterixdb
        $mvn clean package -DskipTests


## Run the build on your machine
Here are steps to get AsterixDB running on your local machine:

* Start a single-machine AsterixDB instance:

        $cd asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-asterixdb-*-SNAPSHOT
        $./opt/local/bin/start-sample-cluster.sh

* Good to go and run queries in your browser at:

        http://localhost:19006

* Read more [documentation](https://ci.apache.org/projects/asterixdb/index.html) to learn the data model, query language, and how to create a cluster instance.

## Documentation

To generate the documentation, run asterix-doc with the generate.rr profile in maven, e.g  `mvn -Pgenerate.rr ...`
Be sure to run `mvn package` beforehand or run `mvn site` in asterix-lang-sqlpp to generate some resources that
are used in the documentation that are generated directly from the grammar.

* [master](https://ci.apache.org/projects/asterixdb/index.html) |
  [0.9.7](http://asterixdb.apache.org/docs/0.9.7/index.html) |
  [0.9.6](http://asterixdb.apache.org/docs/0.9.6/index.html) |
  [0.9.5](http://asterixdb.apache.org/docs/0.9.5/index.html) |
  [0.9.4.1](http://asterixdb.apache.org/docs/0.9.4.1/index.html) |
  [0.9.4](http://asterixdb.apache.org/docs/0.9.4/index.html) |
  [0.9.3](http://asterixdb.apache.org/docs/0.9.3/index.html) |
  [0.9.2](http://asterixdb.apache.org/docs/0.9.2/index.html) |
  [0.9.1](http://asterixdb.apache.org/docs/0.9.1/index.html) |
  [0.9.0](http://asterixdb.apache.org/docs/0.9.0/index.html)

## Community support

- __Users__</br>
  maling list: [users@asterixdb.apache.org](mailto:users@asterixdb.apache.org)</br>
  Join the list by sending an email to [users-subscribe@asterixdb.apache.org](mailto:users-subscribe@asterixdb.apache.org)</br>
- __Developers and contributors__</br>
  mailing list:[dev@asterixdb.apache.org](mailto:dev@asterixdb.apache.org)</br>
  Join the list by sending an email to [dev-subscribe@asterixdb.apache.org](mailto:dev-subscribe@asterixdb.apache.org)
