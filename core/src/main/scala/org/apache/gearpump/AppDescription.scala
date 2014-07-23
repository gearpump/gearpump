package org.apache.gearpump

import akka.actor.Props

/**
 * Created by xzhong10 on 2014/7/22.
 */

case class TaskDescription(task : Props, partitioner : Partitioner)
case class StageDescription(task : TaskDescription, parallism : Int)
case class AppDescription(name : String, stages: Array[StageDescription])
