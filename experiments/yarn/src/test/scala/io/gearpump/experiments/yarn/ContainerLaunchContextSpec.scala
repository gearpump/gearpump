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

package io.gearpump.experiments.yarn

import java.nio.file.{Files, Paths}

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Mockito._
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar

class ContainerLaunchContextSpec extends FlatSpecLike with MockitoSugar {

  "A ContainerLaunchContext object" should "create new ContainerLaunchContext" in {
    val command = "foo"
    val hdfsRoot = "hdfs"
    val mockVersion = "mockVersion"
    val yarnConfig = mock[YarnConfiguration]
    val appConfig = mock[AppConfig]

    when(appConfig.getEnv(Constants.HDFS_ROOT)).thenReturn(hdfsRoot)
    when(appConfig.getEnv("version")).thenReturn(mockVersion)
    when(yarnConfig.get("fs.defaultFS", "file:///")).thenReturn("file:///")

    try {
      Files.createDirectory(Paths.get(hdfsRoot))
      Files.createDirectory(Paths.get(s"$hdfsRoot/conf"))
      Files.createFile(Paths.get(s"$hdfsRoot/mockVersion.tar.gz"))
      ContainerLaunchContext(yarnConfig, appConfig)(command) should not be
        theSameInstanceAs(ContainerLaunchContext(yarnConfig, appConfig)(command))
    } catch {
      case e: Throwable => throw e
    } finally {
      Files.delete(Paths.get(s"$hdfsRoot/mockVersion.tar.gz"))
      Files.delete(Paths.get(s"$hdfsRoot/conf"))
      Files.delete(Paths.get(hdfsRoot))
    }
  }
}
