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

package io.gearpump.jarstore

import com.typesafe.config.Config
import java.io.{InputStream, OutputStream, IOException}

/** Reserve the entire per-artifact allowance before creation, across concurrent uploads. */
private[jarstore] class QuotaJarStore(delegate: JarStore, maxBytes: Long,
    maxFiles: Int, artifactBytes: Long) extends JarStore {
  require(maxBytes > 0 && maxFiles > 0 && artifactBytes > 0 && artifactBytes <= maxBytes)
  override val scheme: String = delegate.scheme
  private var files = delegate.listFiles()
  require(files.values.forall(_ >= 0), "Invalid artifact inventory")
  private var bytes = files.values.foldLeft(0L)(Math.addExact(_, _))
  private var reservations = Set.empty[String]
  override def init(config: Config): Unit = throw new UnsupportedOperationException("Already initialized")
  override def getFile(name: String): InputStream = delegate.getFile(name)
  override def listFiles(): Map[String, Long] = synchronized { files }
  override def deleteFile(name: String): Unit = synchronized {
    require(!reservations.contains(name), "Cannot delete an active upload")
    delegate.deleteFile(name)
    bytes -= files.getOrElse(name, 0L)
    files -= name
  }
  override def createFile(name: String): OutputStream = synchronized {
    JarStore.validateFileName(name)
    if (files.contains(name) || reservations.contains(name) ||
        files.size + reservations.size >= maxFiles || bytes > maxBytes - artifactBytes) {
      throw new IOException("Artifact storage quota exhausted")
    }
    reservations += name
    bytes += artifactBytes
    val output = try delegate.createFile(name) catch {
      case scala.util.control.NonFatal(ex) =>
        reservations -= name
        bytes -= artifactBytes
        throw ex
    }
    new OutputStream {
      private var count = 0L
      private var failed = false
      private var closed = false
      override def write(value: Int): Unit = write(Array(value.toByte), 0, 1)
      override def write(data: Array[Byte], offset: Int, length: Int): Unit = {
        if (closed) throw new IOException("Upload is closed")
        if (length < 0 || count > artifactBytes - length) {
          failed = true
          throw new IOException("Artifact exceeds per-file quota")
        }
        try { output.write(data, offset, length); count += length }
        catch { case scala.util.control.NonFatal(ex) => failed = true; throw ex }
      }
      override def close(): Unit = QuotaJarStore.this.synchronized {
        if (!closed) {
          closed = true
          try output.close() catch {
            case scala.util.control.NonFatal(ex) => failed = true; throw ex
          } finally {
            reservations -= name
            if (failed || count == 0) {
              // Keep the full reservation if physical deletion fails.
              try { delegate.deleteFile(name); bytes -= artifactBytes }
              catch { case scala.util.control.NonFatal(ex) => files += name -> artifactBytes; throw ex }
            } else {
              bytes -= artifactBytes - count
              files += name -> count
            }
          }
        }
      }
    }
  }
}
