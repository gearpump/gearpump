package org.apache.gearpump.streaming.examples.seqfilerw

import org.apache.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import org.apache.gearpump.partitioner.{Partitioner, ShufflePartitioner}
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.{TaskDescription, AppDescription}
import org.apache.gearpump.util.{Configs, Graph}
import org.apache.gearpump.util.Graph._

object SeqFileRW extends App with ArgumentsParser{
  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "seqspout"-> CLIOption[Int]("<spout number>", required = false, defaultValue = Some(2)),
    "seqbolt"-> CLIOption[Int]("<bolt number>", required = false, defaultValue = Some(2)),
    "runseconds" -> CLIOption[Int]("<run seconds>", required = false, defaultValue = Some(60)),
    "input"-> CLIOption[String]("<input file path>", required = true),
    "output"-> CLIOption[String]("<output path>", required = true),
    "stages"-> CLIOption[Int]("<how many stages to run>", required = false, defaultValue = Some(2))
  )

  start()

  def start(): Unit ={
    val config = parse(args)

    val masters = config.getString("master")
    val spout = config.getInt("seqspout")
    val bolt = config.getInt("seqbolt")
    val runseconds = config.getInt("runseconds")
    val stages = config.getInt("stages")
    val input = config.getString("input")
    val output = config.getString("output")

    Console.out.println("Master URL: " + masters)
    val context = ClientContext(masters)

    val appId = context.submit(getApplication(spout, bolt, input, output, stages))
    System.out.println(s"We get application id: $appId")

    Thread.sleep(runseconds * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  def getApplication(spoutNum : Int, boltNum : Int, input : String, output : String, stages : Int) : AppDescription = {
    val config = Configs.empty.withValue(SeqFileSpout.INPUT_PATH, input).withValue(SeqFileBolt.OUTPUT_PATH, output)
    val partitioner = new ShufflePartitioner()
    val spout = TaskDescription(classOf[SeqFileSpout], spoutNum)
    val bolt = TaskDescription(classOf[SeqFileBolt], boltNum)

    var computation : Any = spout ~ partitioner ~> bolt
    computation = 0.until(stages - 2).foldLeft(computation) { (c, id) =>
      c ~ partitioner ~> bolt.copy()
    }

    val dag = Graph[TaskDescription, Partitioner](computation)
    val app = AppDescription("SeqFileReadWrite", config, dag)
    app
  }
}