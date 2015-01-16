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

package org.apache.gearpump.cluster.main

import org.scalatest.{FlatSpec, Matchers}

class ArgumentParserSpec extends FlatSpec with Matchers {
  it should "parse arguments correctly" in {

    val parser = new ArgumentsParser {
      override val options = Array(
        "flag" -> CLIOption[Any]("", required = true),
        "opt1" -> CLIOption[Any]("", required = true),
        "opt2" -> CLIOption[Any]("", required = true))
    }

    val result = parser.parse(Array("-flag" , "-opt1", "1","-opt2", "2", "arg1", "arg2"))
    assert(result.getBoolean("flag"))
    assert(result.getInt("opt1") == 1)
    assert(result.getString("opt1") == "1")
    assert(result.getInt("opt2") == 2)

    assert(result.remainArgs(0) == "arg1")
    assert(result.remainArgs(1) == "arg2")
  }

  it should "handle interleaved options and remain args" in {

    val parser = new ArgumentsParser {
      override val options = Array(
        "opt1" -> CLIOption[Any]("", required = true))
    }

    val result = parser.parse(Array("-opt1", "1","xx.MainClass", "-opt2", "2"))
    assert(result.getInt("opt1") == 1)

    assert(result.remainArgs.length == 3)

    intercept[Exception] {
      parser.parse(Array("-opt2"))
    }

    intercept[Exception] {
      parser.parse(Array("-opt2", "2", "-opt1"))
    }
  }
}
