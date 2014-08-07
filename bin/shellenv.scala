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

val context = ClientContext()

