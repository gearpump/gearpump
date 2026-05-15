## Write your first Gearpump Application

We'll use the classical [wordcount](https://github.com/gearpump/gearpump/tree/master/examples/streaming/wordcount/src/main/scala/io/gearpump/streaming/examples/wordcount) example to illustrate how to write Gearpump applications.

	:::scala     
	/** WordCount with the low-level Processor graph API */
	object WordCount extends PekkoApp with ArgumentsParser {
	
	  override val options: Array[(String, CLIOption[Any])] = Array(
	    "split" -> CLIOption[Int]("<how many source tasks>", required = false,
	      defaultValue = Some(1)),
	    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1))
	  )
	
	  override def main(pekkoConf: Config, args: Array[String]): Unit = {
	    val context = ClientContext(pekkoConf)
	    val config = parse(args)
	    val split = new Split
	    val source = DataSourceProcessor(split, config.getInt("split"), "Split")
	    val sum = Processor[Sum](config.getInt("sum"))
	    val partitioner = new HashPartitioner
	    val computation = source ~ partitioner ~> sum
	    val app = StreamApplication("wordCount", Graph(computation), UserConfig.empty)

	    context.submit(app)
	    context.close()
	  }
	}

This example uses Gearpump's low-level Processor graph API. You build the DAG explicitly with `Processor`, `Partitioner`, `DataSourceProcessor`, and `StreamApplication`.

### IDE Setup (Optional)

You can get your preferred IDE ready for Gearpump by following [this guide](dev-ide-setup).

### Submit application

Finally, you need to package everything into a uber jar with [proper dependencies](http://gearpump.apache.org/downloads.html#maven-dependencies) and submit it to a Gearpump cluster. Please check out the [application submission tool](../introduction/commandline).


