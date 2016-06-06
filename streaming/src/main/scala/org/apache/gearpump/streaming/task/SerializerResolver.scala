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

package org.apache.gearpump.streaming.task

import org.apache.gearpump.esotericsoftware.kryo.util.{IntMap, ObjectMap}
import org.apache.gearpump.streaming.task.SerializerResolver.Registration

private[task] class SerializerResolver {
  private var classId = 0
  private val idToRegistration = new IntMap[Registration]()
  private val classToRegistration = new ObjectMap[Class[_], Registration]()

  def register[T](clazz: Class[T], serializer: TaskMessageSerializer[T]): Unit = {
    val registration = new Registration(classId, clazz, serializer)
    idToRegistration.put(classId, registration)
    classToRegistration.put(clazz, registration)
    classId += 1
  }

  def getRegistration(clazz: Class[_]): Registration = {
    classToRegistration.get(clazz)
  }

  def getRegistration(clazzId: Int): Registration = {
    idToRegistration.get(clazzId)
  }
}

object SerializerResolver {
  class Registration(val id: Int, val clazz: Class[_], val serializer: TaskMessageSerializer[_])
}
