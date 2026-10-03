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

import java.io.File
import java.nio.file.Files
import java.security.MessageDigest

object ArtifactDigest {
  def sha256(file: File): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val in = Files.newInputStream(file.toPath)
    val buffer = new Array[Byte](65536)
    try {
      var read = in.read(buffer)
      while (read != -1) {
        digest.update(buffer, 0, read)
        read = in.read(buffer)
      }
    } finally in.close()
    digest.digest().map(value => f"${value & 0xff}%02x").mkString
  }
  def verify(file: File, expected: String): Unit = {
    require(expected.matches("[0-9a-f]{64}"), "Artifact requires an immutable SHA-256 digest")
    if (!MessageDigest.isEqual(sha256(file).getBytes(java.nio.charset.StandardCharsets.US_ASCII),
        expected.getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
      throw new java.io.IOException("Artifact digest mismatch")
    }
  }
}
