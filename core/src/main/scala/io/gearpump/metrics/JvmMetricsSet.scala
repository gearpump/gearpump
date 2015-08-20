package io.gearpump.metrics

import java.util

import io.gearpump.codahale.metrics.jvm.{ThreadStatesGaugeSet, MemoryUsageGaugeSet}
import io.gearpump.codahale.metrics.{Gauge, Metric, MetricSet}
import scala.collection.JavaConverters._

class JvmMetricsSet(name: String) extends MetricSet {

  override def getMetrics: util.Map[String, Metric] = {
    val memoryMetrics = new MemoryUsageGaugeSet().getMetrics.asScala
    val threadMetrics = new ThreadStatesGaugeSet().getMetrics.asScala
    Map(s"$name.memory.total.used" -> memoryMetrics("total.used"),
        s"$name.memory.total.max" -> memoryMetrics("total.max"),
        s"$name.thread.count" -> threadMetrics("count"),
        s"$name.thread.daemon.count" -> threadMetrics("daemon.count")).asJava
  }
}
