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

package io.gearpump.security

import java.security.{MessageDigest, SecureRandom}
import java.util.Base64
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import scala.util.Try

/** Versioned password hashes; legacy SHA-1 digests must be regenerated. */
object PasswordUtil {
  private val Algorithm = "pbkdf2-sha256"
  private val Iterations = 600000
  private val MaxIterations = 1000000
  private val SaltLength = 16
  private val KeyLength = 32
  private val random = new SecureRandom()

  def hash(password: String): String = {
    require(password != null && password.nonEmpty, "Password must not be empty")
    val salt = new Array[Byte](SaltLength)
    random.nextBytes(salt)
    val key = derive(password, salt, Iterations)
    s"$Algorithm:$Iterations:${encode(salt)}:${encode(key)}"
  }

  def verify(password: String, stored: String): Boolean = {
    Try {
      val (iterations, salt, expected) = parse(stored)
      MessageDigest.isEqual(expected, derive(password, salt, iterations))
    }.getOrElse(false)
  }

  private[security] def isSupportedHash(stored: String): Boolean = {
    Try(parse(stored)).isSuccess
  }

  private def parse(stored: String): (Int, Array[Byte], Array[Byte]) = {
    require(stored != null && stored.length <= 256, "Invalid password hash")
    val fields = stored.split(":", -1)
    require(fields.length == 4 && fields(0) == Algorithm, "Unsupported password hash")
    val iterations = fields(1).toInt
    require(iterations >= Iterations && iterations <= MaxIterations, "Invalid password cost")
    val salt = Base64.getDecoder.decode(fields(2))
    val key = Base64.getDecoder.decode(fields(3))
    require(salt.length == SaltLength && key.length == KeyLength, "Invalid password hash size")
    (iterations, salt, key)
  }

  private def derive(password: String, salt: Array[Byte], iterations: Int): Array[Byte] = {
    val spec = new PBEKeySpec(password.toCharArray, salt, iterations, KeyLength * 8)
    try {
      SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded
    } finally {
      spec.clearPassword()
    }
  }

  private def encode(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)

  // scalastyle:off println
  private def help() = {
    Console.println("usage: gear io.gearpump.security.PasswordUtil -password " +
      "<your password>")
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2 || args(0) != "-password") {
      help()
    } else {
      val pass = args(1)
      val result = hash(pass)
      Console.println("Here is the hashed password")
      Console.println("==============================")
      Console.println(result)
    }
  }
  // scalastyle:on println
}
