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
package io.gearpump.integrationtest

import org.apache.commons.lang.text.{StrMatcher, StrTokenizer}
import org.apache.log4j.Logger
import org.apache.storm.shade.org.eclipse.jetty.util.QuotedStringTokenizer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process._
import scala.collection.JavaConversions._

/**
 * The class is used to execute command in a shell
 */
object ShellExec {

  private val LOG = Logger.getLogger(getClass)
  private val PROCESS_TIMEOUT = 2.minutes

  /**
   * The builtin command line parser by ProcessBuilder (implicit sys.process) don't
   * respect the quote chars (' and ")
   */
  private def splitQuotedString(str: String): List[String] = {
    val splitter = new QuotedStringTokenizer(str, " \t\n\r")
    splitter.asInstanceOf[java.util.Enumeration[String]].toList
  }

  def exec(command: String, sender: String, timeout: Duration = PROCESS_TIMEOUT): Boolean = {
    LOG.debug(s"$sender => `$command`")

    val p = splitQuotedString(command).run()
    val f = Future(blocking(p.exitValue())) // wrap in Future
    val retval = try {
        Await.result(f, timeout)
      } catch {
        case _: TimeoutException =>
          LOG.error(s"timeout to execute command `$command`")
          p.destroy()
          p.exitValue()
      }

    LOG.debug(s"$sender <= exit $retval")
    retval == 0
  }

  def execAndCaptureOutput(command: String, sender: String, timeout: Duration = PROCESS_TIMEOUT): String = {
    LOG.debug(s"$sender => `$command`")

    val buf = new StringBuilder
    val processLogger = ProcessLogger((o: String) => buf.append(o).append("\n"),
      (e: String) => buf.append(e).append("\n"))
    val p = splitQuotedString(command).run(processLogger)
    val f = Future(blocking(p.exitValue())) // wrap in Future
    val retval = try {
        Await.result(f, timeout)
      } catch {
        case _: TimeoutException =>
          p.destroy()
          p.exitValue()
      }
    val output = buf.toString().trim
    val PREVIEW_MAX_LENGTH = 200
    val preview = if (output.length > PREVIEW_MAX_LENGTH)
      output.substring(0, PREVIEW_MAX_LENGTH) + "..." else output

    LOG.debug(s"$sender <= `$preview` exit $retval")
    if (retval != 0) {
      throw new RuntimeException(
        s"exited ($retval) by executing `$command`")
    }
    output
  }

}
