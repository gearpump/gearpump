/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.sql.table;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public class TransactionsTableFactory implements TableFactory<Table> {

  @Override
  public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    final Object[][] rows = {
      {100, "I001", "item1", 3},
      {101, "I002", "item2", 5},
      {102, "I003", "item3", 8},
      {103, "I004", "item4", 33},
      {104, "I005", "item5", 23}
    };

    return new TransactionsTable(ImmutableList.copyOf(rows));
  }

  public static class TransactionsTable implements ScannableTable {

    protected final RelProtoDataType protoRowType = new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder()
          .add("timeStamp", SqlTypeName.TIMESTAMP)
          .add("id", SqlTypeName.VARCHAR, 10)
          .add("item", SqlTypeName.VARCHAR, 50)
          .add("quantity", SqlTypeName.INTEGER)
          .build();
      }
    };

    private final ImmutableList<Object[]> rows;

    public TransactionsTable(ImmutableList<Object[]> rows) {
      this.rows = rows;
    }

    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

  }

}
