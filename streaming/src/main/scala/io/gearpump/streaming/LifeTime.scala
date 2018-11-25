package io.gearpump.streaming

import io.gearpump.Time
import io.gearpump.Time.MilliSeconds

/**
 * Each processor has a LifeTime.
 *
 * When input message's timestamp is beyond current processor's lifetime,
 * then it will not be processed by this processor.
 */
case class LifeTime(birth: MilliSeconds, death: MilliSeconds) {
  def contains(timestamp: MilliSeconds): Boolean = {
    timestamp >= birth && timestamp < death
  }

  def cross(another: LifeTime): LifeTime = {
    LifeTime(Math.max(birth, another.birth), Math.min(death, another.death))
  }
}

object LifeTime {
  val Immortal = LifeTime(Time.MIN_TIME_MILLIS, Time.UNREACHABLE)
}