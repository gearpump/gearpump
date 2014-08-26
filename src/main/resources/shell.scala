import org.apache.gearpump._
import org.apache.gearpump.client.ClientContext
import org.apache.gearpump.util.{ShufflePartitioner, Graph}
import org.apache.gearpump.util.Graph._
import org.apache.gears.cluster.Configs
import org.apache.gearpump.examples.sol._

import org.apache.gearpump.client.ClientContext
import org.apache.gearpump.util.{HashPartitioner, Graph}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.{AppDescription, TaskDescription}
import org.apache.gears.cluster.Configs
import org.apache.gearpump.examples.sol
import org.apache.gearpump.examples.wordcount.WordCount

val context = ClientContext(System.getProperty("masterActorPath"))

class Example {
  def sol(spout : Int, bolt : Int, bytesPerMessage: Int, stages: Int) = SOL.getApplication(spout, bolt, bytesPerMessage, stages)
  def wordcount(split:Int, sum:Int) = new WordCount().getApplication(split, sum)
}

val example = new Example