/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.external.hbase

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.mockito.Mockito
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class HBaseSinkSpec extends PropSpec with PropertyChecks with Matchers {


  property("HBaseSink should insert a row successfully") {
    /*
    import Mockito._
    val htable = Mockito.mock(classOf[HTable])
    val row = "row"
    val group = "group"
    val name = "name"
    val value = "1.2"
    val put = new Put(Bytes.toBytes(row))
    put.add(Bytes.toBytes(group), Bytes.toBytes(name), Bytes.toBytes(value))
    val hbaseSink = HBaseSink(htable)
    hbaseSink.insert(put)
    verify(htable).put(put)
    */
  }
}

