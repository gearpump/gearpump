package org.apache.gearpump

import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import akka.actor._
import org.apache.gearpump.service.SimpleKVService
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Created by xzhong10 on 2014/7/22.
 */
object ActorUtil {
  private val LOG: Logger = LoggerFactory.getLogger(ActorUtil.getClass)

  def getFullPath(context: ActorContext): String = {
    context.self.path.toStringWithAddress(
      context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress)
  }

  def getMaster(system : ActorSystem) : ActorRef = {

    def tryGetMaster(attempt : Int, maxAttempt : Int, interval : Int) : Future[ActorRef] = {
      LOG.info("trying to look up master, attempt: " + attempt);

      if (attempt > maxAttempt) {
        Future.failed(new Exception("max attempt passed"))
      } else {
        val masterURL = SimpleKVService.get("master")
        LOG.info("masterURL: " + masterURL);
        if (masterURL == "" || masterURL == null) {
          Thread.sleep(interval * 1000); // sleep 3s
          tryGetMaster(attempt + 1, maxAttempt, interval)
        } else {
          val master = system.actorSelection(masterURL);
          master.resolveOne(Duration(1, TimeUnit.SECONDS)).recoverWith {
            case ActorNotFound(_) =>
              Thread.sleep(interval * 1000); //sleep 3s
              tryGetMaster(attempt + 1, maxAttempt, interval)
          }
        }
      }
    }

    val master = Await.result(tryGetMaster(0, 10, 3), Duration(1, TimeUnit.MINUTES)).asInstanceOf[ActorRef]
    master
  }

  def defaultMsgHandler : Receive = {
    case msg : Any =>
      LOG.error("Cannot find a matching message, " + msg.getClass.toString)
  }
}
