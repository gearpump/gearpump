package org.apache.gearpump.streaming.examples.seqfilerw

import org.apache.gearpump.streaming.task.{Message, TaskActor}
import org.apache.gearpump.util.Configs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile._
import org.apache.hadoop.io.Text
import org.apache.gearpump.streaming.examples.seqfilerw.SeqFileSpout._
import org.slf4j.{LoggerFactory, Logger}

class SeqFileSpout(config: Configs) extends TaskActor(config ){
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SeqFileSpout])
  val value = new Text()
  val key = new Text()
  var reader: SequenceFile.Reader = null
  val hadoopConf = new Configuration()
  val fs = FileSystem.get(hadoopConf)
  val inputPath = new Path(config.getString(INPUT_PATH))

  override def onStart() = {
    reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    self ! Start
    LOG.info("sequence file spout initiated")
  }

  override def onNext(msg: String) = {
    if(reader.next(key, value)){
      output(key + "++" + value)
    } else {
      reader.close()
      reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    }
    self ! Continue
  }

  override def onStop(): Unit ={
    reader.close()
  }
}

object SeqFileSpout{
  def INPUT_PATH = "inputpath"

  val Start = Message(0, "start")
  val Continue = Message(0, "continue")
}
