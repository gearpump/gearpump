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

import io.gearpump.security.Authenticator.AuthenticationResult
import java.util.concurrent.{ArrayBlockingQueue, RejectedExecutionException, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/** Bound expensive password checks independently of HTTP request processing. */
private[security] class PasswordVerificationExecutor(workers: Int, maxQueued: Int) {
  private val threadIds = new AtomicInteger()
  private val threadFactory = new ThreadFactory {
    override def newThread(work: Runnable): Thread = {
      val thread = new Thread(work, "gearpump-password-verifier-" + threadIds.incrementAndGet())
      thread.setDaemon(true)
      thread
    }
  }
  private val executor = new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](maxQueued), threadFactory,
    new ThreadPoolExecutor.AbortPolicy())

  def verify(check: => AuthenticationResult): Future[AuthenticationResult] = {
    val result = Promise[AuthenticationResult]()
    try {
      executor.execute(new Runnable {
        override def run(): Unit = {
          try {
            result.success(check)
          } catch {
            case NonFatal(error) => result.failure(error)
          }
        }
      })
    } catch {
      // Never execute rejected work on the caller or let the queue grow without bound.
      case _: RejectedExecutionException => result.success(Authenticator.UnAuthenticated)
    }
    result.future
  }

  private[security] def shutdown(): Unit = executor.shutdown()
}
