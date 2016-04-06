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

import org.scalajs.core.tools.io._
import org.scalajs.jsenv.phantomjs.PhantomJSEnv

// https://github.com/scala-js/scala-js/issues/1555
class PhantomJS2Env(jettyClassLoader: ClassLoader,
    phantomjsPath: String = "phantomjs",
    addArgs: Seq[String] = Seq.empty,
    addEnv: Map[String, String] = Map.empty,
    override val autoExit: Boolean = true)
  extends PhantomJSEnv(phantomjsPath, addArgs, addEnv, autoExit, jettyClassLoader) {

  override protected def vmName: String = "PhantomJS2"

  private val consoleNuker = new MemVirtualJSFile("consoleNuker.js")
    .withContent("console.error = undefined;")

  override protected def customInitFiles(): Seq[VirtualJSFile] =
    super.customInitFiles() :+ consoleNuker
}
