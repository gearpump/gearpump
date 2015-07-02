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
package org.apache.gearpump.experiments.parquet.serializer

import org.apache.gearpump.experiments.parquet.User
import org.scalatest.{Matchers, WordSpec}

class GenericAvroSerializerSpec extends WordSpec with Matchers {
  "Serializer" should {
    "do" in {
      val serializer = new GenericAvroSerializer[User](classOf[User])
      val user1 = new User("tom", 23L)
      val bytes1 = serializer.serialize(user1)
      val result1 = serializer.deserialize(bytes1)
      assert(result1 == user1)

      val user2 = new User("adam", 24L)
      val bytes2 = serializer.serialize(user2)
      val result2 = serializer.deserialize(bytes2)
      assert(result2 == user2)
    }
  }
}
