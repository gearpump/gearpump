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

package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump.streaming.appmaster.TaskLocator.Localities
import org.apache.gearpump.streaming.task.TaskId
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

class TaskLocatorSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  it should "serialize/deserialize correctly" in {
    val localities = new Localities(Map(0 -> Array(TaskId(0, 1), TaskId(1,2))))
    Localities.toJson(localities)

    localities.localities.mapValues(_.toList) shouldBe
      Localities.fromJson(Localities.toJson(localities)).localities.mapValues(_.toList)
  }
}
