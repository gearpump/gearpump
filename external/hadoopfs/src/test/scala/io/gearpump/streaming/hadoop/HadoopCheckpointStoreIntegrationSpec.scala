/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.hadoop

import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.hadoop.lib.HadoopUtil
import io.gearpump.streaming.hadoop.lib.rotation.FileSizeRotation
import io.gearpump.streaming.task.TaskId
import io.gearpump.cluster.UserConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class HadoopCheckpointStoreIntegrationSpec extends PropSpec with PropertyChecks with MockitoSugar with Matchers {

  property("HadoopCheckpointStore should persist and recover checkpoints") {
    val fileSizeGen = Gen.chooseNum[Int](100, 1000)
    forAll(fileSizeGen) { (fileSize: Int) =>
      val userConfig = UserConfig.empty
      val taskContext = MockUtil.mockTaskContext
      val hadoopConfig = new Configuration()

      when(taskContext.appId).thenReturn(0)
      when(taskContext.taskId).thenReturn(TaskId(0, 0))

      val rootDirName = "test"
      val rootDir = new Path(rootDirName + Path.SEPARATOR + s"v${HadoopCheckpointStoreFactory.VERSION}")
      val subDir = new Path(rootDir, "app0-task0_0")

      val fs = HadoopUtil.getFileSystemForPath(rootDir, hadoopConfig)
      fs.delete(rootDir, true)
      fs.exists(rootDir) shouldBe false

      val checkpointStoreFactory = new HadoopCheckpointStoreFactory(
        rootDirName, hadoopConfig, new FileSizeRotation(fileSize))
      val checkpointStore = checkpointStoreFactory.getCheckpointStore(userConfig, taskContext)

      checkpointStore.persist(0L, Array(0.toByte))

      val tempFile = new Path(subDir, "checkpoints-0.store")
      fs.exists(tempFile) shouldBe true

      checkpointStore.persist(1L , Array.fill(fileSize)(0.toByte))
      fs.exists(tempFile) shouldBe false
      fs.exists(new Path(subDir, "checkpoints-0-1.store")) shouldBe true

      checkpointStore.persist(2L, Array(0.toByte))
      val newTempFile = new Path(subDir, "checkpoints-2.store")
      fs.exists(newTempFile) shouldBe true

      for (i <- 0 to 2) {
        val optCp = checkpointStore.recover(i)
        optCp should not be empty
      }
      fs.exists(newTempFile) shouldBe false
      fs.exists(new Path(subDir, "checkpoints-2-2.store")) shouldBe true

      checkpointStore.close()
      fs.delete(rootDir, true)
      fs.close()
    }
  }


}
