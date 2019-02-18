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

package io.gearpump.util

import akka.actor._
import com.typesafe.config.Config
import io.gearpump.cluster.ClusterConfig
import io.gearpump.util.LogUtil.ProcessType
import java.util.concurrent.{TimeoutException, TimeUnit}
import org.slf4j.Logger
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
 * ActorSystemBooter start a new JVM process to boot an actor system.
 * All executors are started by ActorSystemBooter
 *
 * It send the system address to "report back actor"
 */
class ActorSystemBooter(config: Config) {
  import io.gearpump.util.ActorSystemBooter._

  def boot(name: String, reportBackActor: String): ActorSystem = {
    val system = ActorSystem(name, config)
    // Daemon path: http://{system}@{ip}:{port}/daemon
    system.actorOf(Props(classOf[Daemon], name, reportBackActor), "daemon")
    system
  }
}

object ActorSystemBooter {

  def apply(config: Config): ActorSystemBooter = new ActorSystemBooter(config)

  def main(args: Array[String]) {
    val name = args(0)
    val reportBack = args(1)
    val config = ClusterConfig.default()

    LogUtil.loadConfiguration(config, ProcessType.APPLICATION)

    val debugPort = Option(System.getProperty(Constants.GEARPUMP_REMOTE_DEBUG_PORT))
    debugPort.foreach { port =>
      val LOG: Logger = LogUtil.getLogger(ActorSystemBooter.getClass)
      LOG.info("==========================================")
      LOG.info("Remote debug port: " + port)
      LOG.info("==========================================")
    }

    val system = apply(config).boot(name, reportBack)

    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        val LOG: Logger = LogUtil.getLogger(ActorSystemBooter.getClass)
        LOG.info("Maybe we have received a SIGINT signal from parent process, " +
          "start to cleanup resources....")
        system.terminate()
      }
    })

    Await.result(system.whenTerminated, Duration.Inf)
  }

  case class BindLifeCycle(actor: ActorRef)
  case class CreateActor(prop: Props, name: String)
  case class ActorCreated(actor: ActorRef, name: String)
  case class CreateActorFailed(name: String, reason: Throwable)

  case class RegisterActorSystem(systemPath: String)

  /**
   * This actor system will watch for parent,
   * If parent dies, this will also die
   */
  case class ActorSystemRegistered(bindLifeWith: ActorRef)
  case class RegisterActorSystemFailed(reason: Throwable)

  object RegisterActorSystemTimeOut

  class Daemon(val name: String, reportBack: String) extends Actor {
    val LOG: Logger = LogUtil.getLogger(getClass, context = name)

    val username = Option(System.getProperty(Constants.GEARPUMP_USERNAME)).getOrElse("not_defined")
    LOG.info(s"RegisterActorSystem to ${reportBack}, current user: $username")

    val reportBackActor = context.actorSelection(reportBack)
    reportBackActor ! RegisterActorSystem(ActorUtil.getSystemAddress(context.system).toString)

    implicit val executionContext = context.dispatcher
    val timeout = context.system.scheduler.scheduleOnce(Duration(25, TimeUnit.SECONDS),
      self, RegisterActorSystemFailed(new TimeoutException))

    context.become(waitForRegisterResult)

    def receive: Receive = null

    def waitForRegisterResult: Receive = {
      case ActorSystemRegistered(parent) =>
        timeout.cancel()
        context.watch(parent)
        context.become(waitCommand)
      case RegisterActorSystemFailed(ex) =>
        LOG.error("RegisterActorSystemFailed", ex)
        timeout.cancel()
        context.stop(self)
    }

    def waitCommand: Receive = {
      case BindLifeCycle(actor) =>
        LOG.info(s"ActorSystem $name Binding life cycle with actor: $actor")
        context.watch(actor)
      case CreateActor(props: Props, name: String) =>
        LOG.info(s"creating actor $name")
        val actor = Try(context.actorOf(props, name))
        actor match {
          case Success(actor) =>
            sender ! ActorCreated(actor, name)
          case Failure(e) =>
            sender ! CreateActorFailed(props.clazz.getName, e)
        }
      case PoisonPill =>
        context.stop(self)
      case Terminated(actor) =>
        LOG.info(s"System $name Watched actor is terminated $actor")
        context.stop(self)
    }

    override def postStop(): Unit = {
      LOG.info(s"ActorSystem $name is shutting down...")
      context.system.terminate()
    }
  }
}
