package io.gearpump

import java.time.Instant

import io.gearpump.Time.MilliSeconds

/**
 * Each message contains an immutable timestamp.
 *
 * For example, if you take a picture, the time you take the picture is the
 * message's timestamp.
 *
 * @param value Accept any type except Null, Nothing and Unit
 */
case class DefaultMessage(value: Any, timeInMillis: MilliSeconds) extends Message {

  /**
   * @param value Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp cannot be larger than Instant.ofEpochMilli(Long.MaxValue)
   */
  def this(value: Any, timestamp: Instant) = {
    this(value, timestamp.toEpochMilli)
  }

  /**
   * Instant.EPOCH is used for default timestamp
   *
   * @param value Accept any type except Null, Nothing and Uni
   */
  def this(value: Any) = {
    this(value, Instant.EPOCH)
  }

  override val timestamp: Instant = {
    Instant.ofEpochMilli(timeInMillis)
  }
}
