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

package org.apache.gearpump.security

import java.security.MessageDigest
import scala.util.Try

import sun.misc.{BASE64Decoder, BASE64Encoder}

/**
 * Util to verify whether user input password is valid or not.
 * It use sha1 to do the digesting.
 */
object PasswordUtil {
  private val SALT_LENGTH = 8

  /**
   * Verifies user input password with stored digest:
   * {{{
   * base64Decode -> extract salt -> do sha1(salt, password) ->
   * generate digest: salt + sha1 -> compare the generated digest with the stored digest.
   * }}}
   */
  def verify(password: String, stored: String): Boolean = {
    Try {
      val decoded = base64Decode(stored)
      val salt = new Array[Byte](SALT_LENGTH)
      Array.copy(decoded, 0, salt, 0, SALT_LENGTH)

      hash(password, salt) == stored
    }.getOrElse(false)
  }
  /**
   * digesting flow (from original password to digest):
   * {{{
   * random salt byte array of length 8 ->
   * byte array of (salt + sha1(salt, password)) -> base64Encode
   * }}}
   */
  def hash(password: String): String = {
    // Salt generation 64 bits long
    val salt = new Array[Byte](SALT_LENGTH)
    new java.util.Random().nextBytes(salt)
    hash(password, salt)
  }

  private def hash(password: String, salt: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-1")
    digest.reset()
    digest.update(salt)
    var input = digest.digest(password.getBytes("UTF-8"))
    digest.reset()
    input = digest.digest(input)
    val withSalt = salt ++ input
    base64Encode(withSalt)
  }

  private def base64Encode(data: Array[Byte]): String = {
    val endecoder = new BASE64Encoder()
    endecoder.encode(data)
  }

  private def base64Decode(data: String): Array[Byte] = {
    val decoder = new BASE64Decoder()
    decoder.decodeBuffer(data)
  }

  // scalastyle:off println
  private def help() = {
    Console.println("usage: gear org.apache.gearpump.security.PasswordUtil -password " +
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