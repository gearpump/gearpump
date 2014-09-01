import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.examples.sol._
import org.apache.gearpump.streaming.examples.wordcount.WordCount

val context = ClientContext(System.getProperty("masterActorPath"))

class Example {
  def sol(spout : Int, bolt : Int, bytesPerMessage: Int, stages: Int) = SOL.getApplication(spout, bolt, bytesPerMessage, stages)
  def wordcount(split:Int, sum:Int) = new WordCount().getApplication(split, sum)
}

val example = new Example