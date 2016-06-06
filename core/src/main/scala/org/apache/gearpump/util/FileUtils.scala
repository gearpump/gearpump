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

package org.apache.gearpump.util

import java.io.{File, IOException}
import java.nio.charset.Charset

import org.apache.gearpump.google.common.io.Files

object FileUtils {
  private val UTF8 = Charset.forName("UTF-8")

  def write(file: File, str: String): Unit = {
    Files.write(str, file, UTF8)
  }

  def read(file: File): String = {
    Files.asCharSource(file, UTF8).read()
  }

  def writeByteArrayToFile(file: File, bytes: Array[Byte]): Unit = {
    Files.write(bytes, file)
  }

  def readFileToByteArray(file: File): Array[Byte] = {
    Files.toByteArray(file)
  }

  /** recursively making all parent directories including itself */
  def forceMkdir(directory: File): Unit = {
    if (directory.exists() && directory.isFile) {
      throw new IOException(s"Failed to create directory ${directory.toString}, it already exist")
    }
    Files.createParentDirs(directory)
    val result = directory.mkdir()
  }
}
