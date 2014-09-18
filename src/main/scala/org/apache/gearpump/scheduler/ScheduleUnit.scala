package org.apache.gearpump.scheduler

import akka.actor.ActorRef

trait ScheduleUnit

case class Resource(slots : Int) extends ScheduleUnit

case class Allocation(slots : Int, worker : ActorRef) extends ScheduleUnit

object Resource{
  implicit def schUnit2Resource(scheduleUnit : ScheduleUnit) = scheduleUnit.asInstanceOf[Resource]
}

object Allocation{
  implicit def allc2Resource(allc : Allocation) = Resource(allc.slots)
}