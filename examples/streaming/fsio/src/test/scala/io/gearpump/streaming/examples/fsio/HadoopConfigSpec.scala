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
package io.gearpump.streaming.examples.fsio

import org.apache.hadoop.conf.Configuration
import org.scalatest.{Matchers, WordSpec}

import io.gearpump.cluster.UserConfig

class HadoopConfigSpec extends WordSpec with Matchers {

  "HadoopConfig" should {
    "serialize and deserialze hadoop configuration properly" in {
      val hadoopConf = new Configuration()
      val key = "test_key"
      val value = "test_value"
      hadoopConf.set(key, value)

      val user = UserConfig.empty

      import io.gearpump.streaming.examples.fsio.HadoopConfig._
      assert(user.withHadoopConf(hadoopConf).hadoopConf.get(key) == value)
    }
  }
}
