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

suite("test_row_store_only_base_compaction", "p0") {
    def tableName = "test_row_store_only_base_compaction"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                `k` INT NOT NULL,
                `v1` VARCHAR(32) NULL,
                `v2` VARCHAR(32) NULL,
                `score` INT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true",
                "row_store_only" = "true",
                "disable_auto_compaction" = "true"
            )
        """

        sql """ INSERT INTO ${tableName} VALUES (1, 'alpha', 'A', 10) """
        sql """ INSERT INTO ${tableName} VALUES (2, 'beta', 'B', 20) """
        sql """ INSERT INTO ${tableName} VALUES (3, 'gamma', 'C', 30) """
        sql """ INSERT INTO ${tableName} VALUES (4, 'delta', NULL, 40) """
        sql """ INSERT INTO ${tableName} VALUES (5, 'epsilon', 'E', 50) """
        sql """ sync """
        trigger_and_wait_compaction(tableName, "cumulative")

        sql """ INSERT INTO ${tableName} VALUES (2, 'beta-updated', 'B2', 200) """
        sql """ DELETE FROM ${tableName} WHERE k = 3 """
        sql """ INSERT INTO ${tableName} VALUES (6, 'zeta', 'Z', 60) """
        sql """ INSERT INTO ${tableName} VALUES (7, 'eta', 'E7', 70) """
        sql """ INSERT INTO ${tableName} VALUES (8, 'theta', NULL, 80) """
        sql """ sync """
        trigger_and_wait_compaction(tableName, "cumulative")

        trigger_and_wait_compaction(tableName, "base")

        order_qt_after_base_compaction """
            SELECT k, v1, v2, score FROM ${tableName} ORDER BY k
        """
        order_qt_after_base_compaction_predicate """
            SELECT k, v1, v2, score FROM ${tableName}
            WHERE v1 LIKE '%ta%' OR v2 IS NULL
            ORDER BY k
        """
    } finally {
        try_sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
