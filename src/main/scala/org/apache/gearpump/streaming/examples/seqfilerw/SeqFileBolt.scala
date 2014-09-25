package org.apache.gearpump.streaming.examples.seqfilerw

import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import org.apache.gearpump.streaming.task.TaskActor
import org.apache.gearpump.util.Configs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.gearpump.streaming.examples.seqfilerw.SeqFileBolt._
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile._
import org.apache.hadoop.io.Text
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.duration.FiniteDuration

class SeqFileBolt(config: Configs) extends TaskActor(config ){
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SeqFileBolt])
  val outputPath = new Path(config.getString(OUTPUT_PATH) + this.taskId)
  var writer: SequenceFile.Writer = null;
  val textClass = new Text().getClass
  val key = new Text()
  val value = new Text()

  private var msgCount : Long = 0
  private var scheduler : Cancellable = null
  private var snapShotKVCount : Long = 0
  private var snapShotTime : Long = 0

  override def onStart() = {
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)
    fs.deleteOnExit(outputPath)
    writer = SequenceFile.createWriter(hadoopConf, Writer.file(outputPath), Writer.keyClass(textClass), Writer.valueClass(textClass))

    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportStatus())
    snapShotTime = System.currentTimeMillis()
    LOG.info("sequence file bolt initiated")
  }

  override def onNext(msg: String): Unit = {
    val kv = msg.split("\\+\\+")
    key.set(kv(0))
    value.set(kv(1))
    writer.append(key, value)
    msgCount += 1
  }

  override def onStop(): Unit ={
    writer.close()
    scheduler.cancel()
    LOG.info("sequence file bolt stopped")
  }

  def reportStatus() = {
    val current : Long = System.currentTimeMillis()
    LOG.info(s"Task $taskId Throughput: ${(msgCount - snapShotKVCount, (current - snapShotTime) / 1000)} (KVPairs, second)")
    snapShotKVCount = msgCTount
    snapShotTime = current
  }
}

object SeqFileBolt{
  val OUTPUT_PATH = "outputpath"
}