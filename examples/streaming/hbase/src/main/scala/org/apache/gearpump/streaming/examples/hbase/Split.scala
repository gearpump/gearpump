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

package org.apache.gearpump.streaming.examples.hbase

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.hadoop.hbase.util.Bytes

class Split extends DataSource {

  private var x = 0

  override def open(context: TaskContext, startTime: Instant): Unit = {}

  override def read(): Message = {

    val tuple = (Bytes.toBytes(s"$x"), Bytes.toBytes("group"),
      Bytes.toBytes("group:name"), Bytes.toBytes("99"))
    x+=1

    Message(tuple)
  }

  override def close(): Unit = {}

  override def getWatermark: Instant = Instant.now()

}
