package io.gearpump.streaming.task

import java.time.Instant

import io.gearpump.Message
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, TimestampedValue}

object TaskUtil {

  /**
   * Resolves a classname to a Task class.
   *
   * @param className  the class name to resolve
   * @return resolved class
   */
  def loadClass(className: String): Class[_ <: Task] = {
    val loader = Thread.currentThread().getContextClassLoader()
    loader.loadClass(className).asSubclass(classOf[Task])
  }

  def trigger[IN, OUT](watermark: Instant, runner: StreamingOperator[IN, OUT],
      context: TaskContext): Unit = {
    val triggeredOutputs = runner.trigger(watermark)
    context.updateWatermark(triggeredOutputs.watermark)
    triggeredOutputs.outputs.foreach { case TimestampedValue(v, t) =>
      context.output(Message(v, t))
    }
  }

  /**
   * @return t1 if t1 is not larger than t2 and t2 otherwise
   */
  def min(t1: Instant, t2: Instant): Instant = {
    if (t1.isAfter(t2)) t2
    else t1
  }

  /**
   * @return t1 if t1 is not smaller than t2 and t2 otherwise
   */
  def max(t1: Instant, t2: Instant): Instant = {
    if (t2.isBefore(t1)) t1
    else t2
  }
}
