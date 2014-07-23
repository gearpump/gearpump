package org.apache.gearpump


import java.util.concurrent.TimeUnit
import akka.util._
import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.Future
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Created by xzhong10 on 2014/7/22.
 */
class Executor(worker : ActorRef, master : ActorRef, executorId : Int, slots: Int)  extends Actor {
  import Executor._
  def receive : Receive = {
    case LaunchTask(taskId, taskDescription, outputs) =>
      LOG.info("Launching Task " + taskId + ", " + taskDescription.toString + ", " + outputs.toString())
      val task = context.actorOf(taskDescription.task, taskId.toString)
      sender ! TaskLaunched(taskId, task)
  }

  override def preStart : Unit = {
    LOG.info("Registering Executor, id: " + executorId + ", slots: " + slots)
    worker ! RegisterExecutor(master, executorId, slots)
  }
}

object Executor {
  private val LOG: Logger = LoggerFactory.getLogger(Executor.getClass)

  def main(args: Array[String]) {
    val command = args.mkString(" ")
    println("Starting executor..." + args.mkString(" "))

    val system = ActorSystem("executor", Configs.SYSTEM_DEFAULT_CONFIG)

    val appId = args(0).toInt
    val executorId = args(1).toInt
    val slots = args(2).toInt
    val workerPath = args(3)
    val appMasterPath = args(4)

    LOG.info("appid: " + appId + ", executor Id;" + executorId + ", slots: " + slots + ", worker: " + workerPath + "master: " + appMasterPath)

    implicit val timeout = new Timeout(3, TimeUnit.SECONDS);
    val workerFuture = system.actorSelection(workerPath).resolveOne()
    val masterFuture = system.actorSelection(appMasterPath).resolveOne()

    val workerAndMaster = for {worker <- workerFuture
      master <- masterFuture} yield (worker, master)

    LOG.info("executor process is starting...");

    workerAndMaster.onComplete {
      case Success((worker, master)) =>
        val actor = system.actorOf(Props(classOf[Executor], worker, master, executorId, slots), "executor")
      case Failure(t) =>
        LOG.error("An error has occured: " + t.getMessage)
        LOG.info("system shutdown due to failure...");
        system.shutdown()
    }
    system.awaitTermination()
    LOG.info("executor process is shutdown...");
  }
}
