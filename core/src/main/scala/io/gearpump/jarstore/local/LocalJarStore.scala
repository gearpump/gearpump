/*
 * Licensed under the Apache License, Version 2.0 (the
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
package io.gearpump.jarstore.local

import com.typesafe.config.Config
import io.gearpump.jarstore.JarStore
import io.gearpump.util.{Constants, FileUtils, LogUtil, Util}
import java.io._
import java.nio.file.{Files, LinkOption, NoSuchFileException, StandardOpenOption}
import org.slf4j.Logger

/**
 * LocalJarStore store the uploaded jar on local disk.
 */
class LocalJarStore extends JarStore {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private var rootPath: File = _
  override val scheme: String = "file"

  class ClosedInputStream extends InputStream {
    override def read(): Int = -1
  }

  override def init(config: Config): Unit = {
    rootPath = Util.asSubDirOfGearpumpHome(
      config.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH))
    createDirIfNotExists(rootPath)
  }

  /**
   * Creates the file on JarStore.
   *
   * @param fileName  name of the file to be created on JarStore.
   * @return OutputStream returns a stream into which the data can be written.
   */
  override def createFile(fileName: String): OutputStream = {
    JarStore.validateFileName(fileName)
    createDirIfNotExists(rootPath)
    val localFile = new File(rootPath, fileName)
    Files.newOutputStream(localFile.toPath, StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)
  }

  /**
   * Gets the InputStream to read the file
   *
   * @param fileName name of the file to be read on JarStore.
   * @return InputStream returns a stream from which the data can be read.
   */
  override def getFile(fileName: String): InputStream = {
    JarStore.validateFileName(fileName)
    val localFile = new File(rootPath, fileName)
    val is = try {
      Files.newInputStream(localFile.toPath, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
    } catch {
      case ex: NoSuchFileException =>
        LOG.error(s"Fetch file $fileName failed: ${ex.getStackTrace}")
        new ClosedInputStream
    }
    is
  }

  override def listFiles(): Map[String, Long] = {
    import scala.jdk.CollectionConverters._
    val paths = Files.list(rootPath.toPath)
    try paths.iterator().asScala.map { path =>
      require(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS),
        "Artifact root must contain only regular files")
      JarStore.validateFileName(path.getFileName.toString)
      path.getFileName.toString -> Files.size(path)
    }.toMap finally paths.close()
  }

  override def deleteFile(fileName: String): Unit = {
    JarStore.validateFileName(fileName)
    Files.deleteIfExists(rootPath.toPath.resolve(fileName))
  }

  private def createDirIfNotExists(file: File): Unit = {
    if (!file.exists()) {
      FileUtils.forceMkdir(file)
    }
  }
}