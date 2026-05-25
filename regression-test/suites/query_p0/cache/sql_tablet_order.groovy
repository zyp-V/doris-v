// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

import java.util.stream.Collectors

suite("sql_tablet_order") {
    // TODO: regression-test does not support check query profile,
    // so this suite does not check whether cache is used, :)
    def tableName = "test_tablet_order"

    def variables = sql "show variables"
    def variableString = variables.stream()
            .map { it.toString() }
            .collect(Collectors.joining("\n"))
    logger.info("Variables:\n${variableString}")

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int NOT NULL COMMENT "",
              `pt` date NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `pt`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`pt`)
            (PARTITION p202205 VALUES [('2022-05-01'), ('2022-06-01')),
            PARTITION p202206 VALUES [('2022-06-01'), ('2022-07-01')))
            DISTRIBUTED BY HASH(`k1`, `pt`) BUCKETS 2
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """

    sql "sync"

    sql """ INSERT INTO ${tableName} VALUES 
                    (1,"2022-05-27"),
                    (2, "2022-05-28"),
                    (3, "2022-05-29"),
                    (4, "2022-05-30"),
                    (5, "2022-06-01"),
                    (6, "2022-06-01")
        """

    sql "set enable_sql_cache=true "
    sql "SET enable_nereids_planner=true"

    def q1 = """
                    select
                        k1,
                        pt
                    from
                        ${tableName} partition(p202205) bucket(1)
                    where
                        pt between '2022-05-28' and '2022-06-30' 
                    order by
                        k1;
                """

    qt_sql_cache1 q1

    qt_sql_cache2 q1

    qt_sql_cache3 """
                    select
                        k1,
                        pt
                    from
                        ${tableName} partition(p202205) bucket(0)
                    where
                        pt between '2022-05-28' and '2022-06-30' 
                    order by
                        k1;
                """

    test {
        sql """select k1 from ${tableName} partition(p202205) bucket(-1) """
        exception "java.sql.SQLException"
    }

    test {
        sql """select k1 from ${tableName} partition(p202205) bucket(2) """
        exception "java.sql.SQLException"
    }

    sql "SET enable_nereids_planner=false"

    def q4 = """
                    select
                        k1,
                        pt
                    from
                        ${tableName} bucket(0) t1
                    where
                        pt between '2022-05-28' and '2022-05-30' 
                    order by
                        k1;
                """

    qt_sql_cache4 q4
    qt_sql_cache5 q4

    test {
        sql """select k1 from ${tableName} partition(p202205) bucket(-1) """
        exception "java.sql.SQLException"
    }

    test {
        sql """select k1 from ${tableName} partition(p202205) bucket(2) """
        exception "java.sql.SQLException"
    }
}
