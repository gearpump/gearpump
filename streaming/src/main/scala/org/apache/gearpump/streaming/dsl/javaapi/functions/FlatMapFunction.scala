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
package org.apache.gearpump.streaming.dsl.javaapi.functions

import org.apache.gearpump.streaming.dsl.api.functions.SerializableFunction

/**
 * Transforms one input into zero or more outputs of possibly different types.
 * This Java version of FlatMapFunction returns a java.util.Iterator.
 *
 * @tparam T Input value type
 * @tparam R Output value type
 */
abstract class FlatMapFunction[T, R] extends SerializableFunction {

  def flatMap(t: T): java.util.Iterator[R]
}
