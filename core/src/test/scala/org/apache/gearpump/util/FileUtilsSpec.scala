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

import java.io.File
import java.util

import org.scalatest.FlatSpec

import org.apache.gearpump.google.common.io.Files

class FileUtilsSpec extends FlatSpec {
  val TXT =
    """
      |This is a multiple line
      |text
      |
    """.stripMargin

  it should "read/write string correctly" in {
    val file = File.createTempFile("fileutilspec", ".test")
    FileUtils.write(file, TXT)
    assert(FileUtils.read(file) == TXT)
    file.delete()
  }

  it should "read/write bytes array correctly" in {
    val file = File.createTempFile("fileutilspec", ".test")
    val bytes = TXT.toCharArray.map(_.toByte)
    FileUtils.writeByteArrayToFile(file, bytes)
    util.Arrays.equals(bytes, FileUtils.readFileToByteArray(file))
    file.delete()
  }

  it should "create directory and all parents" in {
    val temp = Files.createTempDir()
    val parent = new File(temp, "sub1")
    val child = new File(parent, "sub2" + File.separator)
    FileUtils.forceMkdir(child)
    assert(child.exists())
    assert(child.isDirectory)
    child.delete()
    parent.delete()
    temp.delete()
  }
}