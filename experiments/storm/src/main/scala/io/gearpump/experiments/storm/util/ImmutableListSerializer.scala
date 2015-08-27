/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.storm.util

import com.google.common.collect.ImmutableList
import io.gearpump.esotericsoftware.kryo.io.{Input, Output}
import io.gearpump.esotericsoftware.kryo.{Kryo, Serializer}

class ImmutableListSerializer extends Serializer[ImmutableList[AnyRef]] {
  override def write(kryo: Kryo, output: Output, list: ImmutableList[AnyRef]): Unit = {
    output.writeInt(list.size, true)
    val iterator = list.iterator()
    while (iterator.hasNext) {
      kryo.writeClassAndObject(output, iterator.next())
    }
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[ImmutableList[AnyRef]]): ImmutableList[AnyRef] = {
    val size = input.readInt(true)
    val array = new Array[AnyRef](size)
    for (i <- 0 until size) {
      array(i) = kryo.readClassAndObject(input)
    }
    ImmutableList.copyOf(array)
  }
}

