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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class TableStreamManagerTest {

    @Test
    public void testAddAndRemoveTableStreamDoesNotResolveDatabase() {
        TableStreamManager manager = new TableStreamManager();
        BaseTableStream stream = new UnresolvedDbTableStream();
        stream.setId(10L);
        stream.setName("s1");
        stream.setQualifiedDbName("test_db");

        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        Mockito.when(db.getId()).thenReturn(1L);

        manager.addTableStream(1L, stream);
        Assertions.assertEquals(Collections.singleton(10L), manager.getTableStreamIds(db));

        manager.removeTableStream(1L, stream);
        Assertions.assertTrue(manager.getTableStreamIds(db).isEmpty());
    }

    private static class UnresolvedDbTableStream extends BaseTableStream {
        @Override
        public DatabaseIf getDatabase() {
            throw new AssertionError("TableStreamManager should not resolve database during image replay");
        }

        @Override
        void fillTableStreamConsumptionInfo(List<TRow> dataBatch) {
        }

        @Override
        public void unprotectedCheckStreamUpdate(AbstractTableStreamUpdate update) throws UserException {
        }

        @Override
        public void unprotectedUpdateStreamUpdate(AbstractTableStreamUpdate update, Long ts) {
        }
    }
}
