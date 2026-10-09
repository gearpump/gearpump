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

import io.gearpump.cluster.TestUtil
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, Multipart, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

class FileDirectiveSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private implicit val system: ActorSystem =
    ActorSystem("FileDirectiveSpec", TestUtil.DEFAULT_CONFIG)
  private implicit val mat: Materializer = SystemMaterializer(system).materializer
  private implicit val ec: ExecutionContext = system.dispatcher

  override def afterAll(): Unit = {
    try Await.result(system.terminate(), 10.seconds) finally super.afterAll()
  }

  it should "discard an excess upload body and recover parser capacity" in {
    val run = Route.toFunction(FileDirective.uploadFile { _ => complete("parsed") })
    val release = Promise[ByteString]()
    val subscribed = Vector.fill(8)(Promise[Unit]())
    val active = subscribed.map { started =>
      val data = Source.single(ByteString("held")).map { bytes =>
        started.trySuccess(())
        bytes
      }.concat(Source.future(release.future))
      val part = Multipart.FormData.BodyPart("args",
        HttpEntity.IndefiniteLength(ContentTypes.`text/plain(UTF-8)`, data))
      run(HttpRequest(method = HttpMethods.POST, uri = "/",
        entity = Multipart.FormData(Source.single(part)).toEntity))
    }
    try {
      Await.result(Future.sequence(subscribed.map(_.future)), 10.seconds)
      val consumed = Promise[Unit]()
      val data = Source.single(ByteString("excess")).watchTermination() { (value, completed) =>
        completed.foreach(_ => consumed.trySuccess(()))
        value
      }
      val part = Multipart.FormData.BodyPart("args",
        HttpEntity.IndefiniteLength(ContentTypes.`text/plain(UTF-8)`, data))
      val response = Await.result(run(HttpRequest(method = HttpMethods.POST, uri = "/",
        entity = Multipart.FormData(Source.single(part)).toEntity)), 10.seconds)
      assert(response.status == StatusCodes.ServiceUnavailable)
      response.discardEntityBytes()
      Await.result(consumed.future, 5.seconds)
    } finally {
      release.trySuccess(ByteString("released"))
      Await.result(Future.sequence(active), 10.seconds).foreach { response =>
        assert(response.status == StatusCodes.OK)
        response.discardEntityBytes()
      }
    }
    val part = Multipart.FormData.BodyPart.Strict("args", HttpEntity("finished"))
    val response = Await.result(run(HttpRequest(method = HttpMethods.POST, uri = "/",
      entity = Multipart.FormData(Source.single(part)).toEntity)), 10.seconds)
    assert(response.status == StatusCodes.OK)
    response.discardEntityBytes()
  }
}
