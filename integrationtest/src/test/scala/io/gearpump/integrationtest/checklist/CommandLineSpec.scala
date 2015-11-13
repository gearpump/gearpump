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
package io.gearpump.integrationtest.checklist

import io.gearpump.integrationtest.TestSpecBase

/**
 * The test spec checks the command-line usage
 */
trait CommandLineSpec extends TestSpecBase {

  "command `gear app`" should "submit an user application" in {
  }

  "command `gear info $app_id`" should "return particular application runtime information" in {
  }

  "command `gear info $wrong_app_id`" should "return an error" in {
  }

  "command `gear kill $app_id`" should "kill particular application" in {
  }

  "command `gear kill $wrong_app_id`" should "return an error" in {
  }

  "command `gear replay $app_id`" should "return replay the application from current min clock" in {
  }

}
