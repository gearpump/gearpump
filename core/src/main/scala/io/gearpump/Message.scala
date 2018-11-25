package io.gearpump

import java.time.Instant

import io.gearpump.Time.MilliSeconds

trait Message {

  val value: Any

  val timestamp: Instant
}

object Message {

  /**
   * Instant.EPOCH is used for default timestamp
   *
   * @param value Accept any type except Null, Nothing and Unit
   */
  def apply(value: Any): Message = {
    new DefaultMessage(value)
  }

  /**
   * @param value Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp must be smaller than Long.MaxValue
   */
  def apply(value: Any, timestamp: MilliSeconds): Message = {
    DefaultMessage(value, timestamp)
  }

  /**
   * @param value Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp must be smaller than Instant.ofEpochMilli(Long.MaxValue)
   */
  def apply(value: Any, timestamp: Instant): Message = {
    new DefaultMessage(value, timestamp)
  }
}