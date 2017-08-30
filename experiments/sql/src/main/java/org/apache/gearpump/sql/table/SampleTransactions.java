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

public class SampleTransactions {

  public static class Transactions {

    public final Order[] ORDERS = {
      new Order("001", 3),
      new Order("002", 5),
      new Order("003", 8),
      new Order("004", 15),
    };

    public final Product[] PRODUCTS = {
      new Product("001", "Book"),
      new Product("002", "Pen"),
      new Product("003", "Pencil"),
      new Product("004", "Ruler"),
    };
  }

  public static class Order {
    public final String ID;
    public final int QUANTITY;

    public Order(String ID, int QUANTITY) {
      this.ID = ID;
      this.QUANTITY = QUANTITY;
    }
  }

  public static class Product {
    public final String ID;
    public final String NAME;

    public Product(String ID, String NAME) {
      this.ID = ID;
      this.NAME = NAME;
    }
  }

}
