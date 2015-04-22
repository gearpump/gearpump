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

package org.apache.gearpump.experiments.storm.util

import java.io.{FileOutputStream, IOException, File}
import java.util.jar.JarFile
import java.util.{List => JList, LinkedList => JLinkedList, Map => JMap, HashMap => JHashMap}

import backtype.storm.generated.{ComponentCommon, StormTopology}
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.Fields
import clojure.lang.Atom
import org.apache.commons.io.{IOUtils, FileUtils}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object TopologyContextBuilder {
  private val LOG: Logger = LogUtil.getLogger(classOf[TopologyContextBuilder])

  def apply(topology: StormTopology, stormConf: JMap[_, _], multiLang: Boolean): TopologyContextBuilder = {
    if (multiLang) {
      new TopologyContextBuilder(topology, stormConf, mkCodeDir, mkPidDir)
    } else {
      new TopologyContextBuilder(topology, stormConf)
    }
  }

  // a workaround to support storm ShellBolt
  private def mkPidDir: String = {
    val pidDir = FileUtils.getTempDirectoryPath + File.separator + "pid"
    try {
      FileUtils.forceMkdir(new File(pidDir))
    } catch {
      case ex: IOException =>
        LOG.error(s"failed to create pid directory $pidDir")
    }
    pidDir
  }

  // a workaround to support storm ShellBolt
  private def mkCodeDir: String = {
    val jarPath = System.getProperty("java.class.path").split(":").last
    val destDir = FileUtils.getTempDirectoryPath + File.separator + "storm"

    try {
      FileUtils.forceMkdir(new File(destDir))

      val jar = new JarFile(jarPath)
      val enumEntries = jar.entries()
      enumEntries.foreach { entry =>
        val file = new File(destDir + File.separator + entry.getName)
        if (!entry.isDirectory) {
          file.getParentFile.mkdirs()

          val is = jar.getInputStream(entry)
          val fos = new FileOutputStream(file)
          try {
            IOUtils.copy(is, fos)
          } catch {
            case ex: IOException =>
              LOG.error(s"failed to copy data from ${entry.getName} to ${file.getName}")
          } finally {
            fos.close()
            is.close()
          }
        }
      }
    } catch {
      case ex: IOException =>
        LOG.error(s"could not extract $destDir from $jarPath")
    }

    destDir + File.separator + "resources"
  }
}

class TopologyContextBuilder(topology: StormTopology,
                             stormConf: JMap[_, _],
                             codeDir: String = null,
                             pidDir: String = null)  {

  private val componentToStreamFields = StormUtil.getComponentToStreamFields(topology)

  def buildContext(taskId: Int, componentId: String): TopologyContext = {
    val taskToComponent = new JHashMap[Integer, String]
    taskToComponent.put(taskId, componentId)
    val sortedTasks: JList[Integer] = new JLinkedList[Integer]
    sortedTasks.add(taskId)
    val componentToSortedTasks = new JHashMap[String, JList[Integer]]
    componentToSortedTasks.put(componentId, sortedTasks)
    new TopologyContext(topology, stormConf, taskToComponent, componentToSortedTasks,
      componentToStreamFields, null, codeDir, pidDir, taskId, null, null, null, null, null,
      new JHashMap[AnyRef, AnyRef], new Atom(false)
    )
  }


}
