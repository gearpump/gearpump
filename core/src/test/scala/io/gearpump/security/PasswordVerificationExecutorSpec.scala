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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._

class PasswordVerificationExecutorSpec extends AnyFlatSpec with Matchers {
  it should "bound running and queued checks and reject overload without evaluating it" in {
    val verifier = new PasswordVerificationExecutor(1, 1)
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val queuedCalls = new AtomicInteger()
    val rejectedCalls = new AtomicInteger()
    try {
      val running = verifier.verify {
        assert(Thread.currentThread().getName.startsWith("gearpump-password-verifier-"))
        started.countDown()
        assert(release.await(10, TimeUnit.SECONDS))
        Authenticator.Admin
      }
      assert(started.await(5, TimeUnit.SECONDS))
      val queued = verifier.verify {
        queuedCalls.incrementAndGet()
        Authenticator.User
      }
      val rejected = verifier.verify {
        rejectedCalls.incrementAndGet()
        Authenticator.Guest
      }
      Await.result(rejected, 1.second) shouldBe Authenticator.UnAuthenticated
      rejectedCalls.get() shouldBe 0
      queuedCalls.get() shouldBe 0
      release.countDown()
      Await.result(running, 5.seconds) shouldBe Authenticator.Admin
      Await.result(queued, 5.seconds) shouldBe Authenticator.User
      queuedCalls.get() shouldBe 1
      Await.result(verifier.verify(Authenticator.Guest), 5.seconds) shouldBe Authenticator.Guest
    } finally {
      release.countDown()
      verifier.shutdown()
    }
  }

  it should "complete failed checks and keep the verification worker available" in {
    val verifier = new PasswordVerificationExecutor(1, 1)
    try {
      val failure = verifier.verify(throw new IllegalArgumentException("invalid check"))
      intercept[IllegalArgumentException] { Await.result(failure, 5.seconds) }
      Await.result(verifier.verify(Authenticator.Admin), 5.seconds) shouldBe Authenticator.Admin
    } finally {
      verifier.shutdown()
    }
  }
}
