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
package org.apache.gearpump.external.hbase.dsl

import org.apache.gearpump.external.hbase.HBaseSink
import org.apache.gearpump.streaming.dsl.{TypedDataSink, Stream}
import org.apache.gearpump.streaming.dsl.Stream.Sink

import scala.reflect.ClassTag

class HBaseDSLSink[T: ClassTag](stream: Stream[T]) {
  def writeToHbase(table: String, parallism: Int, description: String): Stream[T] = {
    stream.sink(new HBaseSink(table) with TypedDataSink[T], parallism, description)
  }
}

object HBaseDSLSink {
  implicit def streamToHBaseDSLSink[T: ClassTag](stream: Stream[T]): HBaseDSLSink[T] = {
    new HBaseDSLSink[T](stream)
  }
}
