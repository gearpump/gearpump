package io.gearpump.streaming.task

import akka.actor.ActorRef
import io.gearpump.streaming.LifeTime

case class TaskContextData(
    executorId: Int,
    appId: Int,
    appName: String,
    appMaster: ActorRef,
    parallelism: Int,
    life: LifeTime,
    subscribers: List[Subscriber])
