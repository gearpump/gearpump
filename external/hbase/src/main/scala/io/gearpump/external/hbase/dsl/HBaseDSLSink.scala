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
package io.gearpump.external.hbase.dsl

import org.apache.hadoop.conf.Configuration
import io.gearpump.cluster.UserConfig
import io.gearpump.external.hbase.HBaseSink
import io.gearpump.streaming.dsl.Stream
import Stream.Sink

class HBaseDSLSink[T](stream: Stream[T]) {
  def writeToHbase(userConfig: UserConfig, table: String, parallism: Int, description: String): Stream[T] = {
    stream.sink(HBaseSink[T](userConfig, table), parallism, userConfig, description)
  }

  def writeToHbase(userConfig: UserConfig, configuration: Configuration, table: String, parallism: Int, description: String): Stream[T] = {
    stream.sink(HBaseSink[T](userConfig, table, configuration), parallism, userConfig, description)
  }
}

object HBaseDSLSink {
  implicit def streamToHBaseDSLSink[T](stream: Stream[T]): HBaseDSLSink[T] = {
    new HBaseDSLSink[T](stream)
  }
}
