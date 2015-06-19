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

package org.apache.gearpump.streaming.sink

/**
 * interface to implement custom data sink
 * which the result of a DAG is typically written to
 *
 * an example would be like
 * {{{
 *  class ConsoleSink extends DataSink[String] {
 *
 *    def write(s: String): Unit = {
 *      Console.println(s)
 *    }
 *
 *    def close(): Unit = {}
 *  }
 * }}}
 * @tparam T type of data written to sink
 */
trait DataSink[T] {

  def write(t: T): Unit

  def close(): Unit
}
