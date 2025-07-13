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

## 🧠 Current Query Execution & Memory Management in AsterixDB

AsterixDB follows a structured pipeline for query execution and memory allocation. Below is an overview of the process:

### 1. Query Plan Generation
- When a query is submitted, AsterixDB generates an **optimized logical query plan**.
- Optimization is done using the **Cost-Based Optimizer (CBO)**.

### 2. Memory Assignment
- The memory budget is configured in the `cc.conf` configuration file.
- The rule class `SetMemoryRequirementsRule.java` assigns memory to each operator in the query plan.
- Memory assignment is based on:
  - Operator type
  - Estimated input data size (from CBO stats if available)

### 3. Plan Stage Conversion
- The logical plan is converted into **plan stages** via the **Plan Stages Generator**.

### 4. Hyracks Job Generation
- Plan stages are compiled into a **Hyracks job**, which represents the physical execution of the query.

### 5. Memory Requirement Estimation
- Total query memory requirements are computed by summing the budgets of all operators.
- This value is used to evaluate whether the query can be admitted.

### 6. Admission Control
- The **Admission Controller** checks whether the system has sufficient resources to admit the query.
- If yes, the query is accepted for execution.

### 7. Scheduling & Execution
- The **Scheduler** either:
  - Executes the query immediately if resources are available, or
  - Queues the query for later execution.

---

This process ensures efficient memory use and predictable performance under concurrent query loads.

---

## What We Changed: Introducing CBO-Based Memory Allocation

To improve memory estimation, we introduced **Cost-Based Optimizer (CBO)**-driven memory budgeting for memory-intensive operators. This allows the system to assign more accurate memory based on operator statistics, reducing the chance of unnecessary spilling or under-utilization.

### Memory-Intensive Operators
The following operators are now allocated memory more precisely:
- `Join`
- `GroupBy`
- `OrderBy`
- `Window`
- `TextSearch`

---

## CBO-Based Memory Allocation

### `CBOMemoryBudget.java`

For each memory-intensive operator, three types of memory budgets are computed (if CBO is enabled):

1. **Default Memory Budget**
  - Used when no CBO statistics are available.
  - Configurable via `cc.conf`.

2. **CBO-Based Max Memory**
  - Used when `compiler.cbo = true` and sufficient memory is available.
  - Allocates the ideal memory to avoid spilling and maximize in-memory processing.

3. **CBO-Based Optimal Memory**
  - Used when `compiler.cbo = true`, but max memory isn't available.
  - Allocates a “good enough” amount of memory to minimize spilling and balance system usage.

> 💡 *Note:* AsterixDB's current architecture does not support dynamic memory resizing after admission. Hence, all memory budgets must be determined **before query execution**.

---

###  Working Mechanism

1. **Memory Budget Calculation**
  - During optimization, in `SetMemoryRequirementsRule.java`, memory is assigned to each operator.
  - For memory-intensive operators, all three memory budgets are populated.

2. **Plan Stage Generation**
  - These budgets propagate through the Plan Stages Generator and influence overall query memory needs.

3. **Admission Decision**
  - At admission time, if CBO is enabled:
    - The system first tries to allocate the **CBO-Based Max Memory**.
    - If not possible, it falls back to **CBO-Based Optimal Memory**.
    - If still not feasible, the **Default Memory Budget** is used.

4. **User-Defined Budget (Override)**
  - If a user explicitly sets a memory budget for an operator, that value **overrides** all CBO-based budgets.

---

📂 **See:**  
Implementation logic for memory estimation can be found in [`SetMemoryRequirementsRule.java`](hyracks-fullstack/algebricks/algebricks-rewriter/src/main/java/org/apache/hyracks/algebricks/rewriter/rules/SetMemoryRequirementsRule.java).
---
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

