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
package org.apache.gearpump.jarstore

import java.io._

import org.apache.gearpump.cluster.UserConfig
import JarFileContainerWrapper._

/**
 *  The JarFileContainer are supposed to only be used by daemon class so the real implementation
 *  are in project Daemon. However, the AppJar is also transfered by ApplicationMaster which has
 *  a JarFileContainer. To avoid the serialization of JarFileContainer implementation in
 *  application layer, we use JarFileContainerWrapper to wrap the real object.
 */

class JarFileContainerWrapper private (conf: UserConfig) extends JarFileContainer{

  def this(realContainer: JarFileContainer) = {
    this(UserConfig.empty.withBytes(REAL_JAR_CONTAINER, serializeJarFileContainer(realContainer)))
  }

  override def copyFromLocal(localFile: File): Unit = {
    jarContainer.copyFromLocal(localFile)
  }

  override def copyToLocalFile(localPath: File): Unit = {
    jarContainer.copyToLocalFile(localPath)
  }

  @transient
  private var _jarContainer: JarFileContainer = null

  private[jarstore] def jarContainer: JarFileContainer = {
    if(_jarContainer == null) {
      _jarContainer = deserializeJarFileContainer(conf.getBytes(REAL_JAR_CONTAINER).get)
    }
    _jarContainer
  }
}

object JarFileContainerWrapper{
  val REAL_JAR_CONTAINER = "jarcontainer"

  def serializeJarFileContainer(jarfileContainer: JarFileContainer): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(baos)
    out.writeObject(jarfileContainer)
    baos.toByteArray
  }

  def deserializeJarFileContainer(bytes: Array[Byte]): JarFileContainer = {
    val in = new ObjectInputStream(new ByteArrayInputStream(bytes))
    in.readObject().asInstanceOf[JarFileContainer]
  }
}