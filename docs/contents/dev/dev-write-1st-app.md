## Write your first Gearpump Application

We'll use the classical [wordcount](https://github.com/apache/incubator-gearpump/tree/master/examples/streaming/wordcount/src/main/scala/org/apache/gearpump/streaming/examples/wordcount) example to illustrate how to write Gearpump applications.

	:::scala     
	/** WordCount with High level DSL */
	object WordCount extends AkkaApp with ArgumentsParser {
	
	  override val options: Array[(String, CLIOption[Any])] = Array.empty
	
	  override def main(akkaConf: Config, args: Array[String]): Unit = {
	    val context = ClientContext(akkaConf)
	    val app = StreamApp("dsl", context)
	    val data = "This is a good start, bingo!! bingo!!"
	
	    //count for each word and output to log
	    app.source(data.lines.toList, 1, "source").
	      // word => (word, count)
	      flatMap(line => line.split("[\\s]+")).map((_, 1)).
	      // (word, count1), (word, count2) => (word, count1 + count2)
	      groupByKey().sum.log
	
        context.submit(app).waitUntilFinish()
        context.close()
	  }
	}

The example is written in our [Stream DSL](http://gearpump.apache.org/releases/latest/api/scala/index.html#org.apache.gearpump.streaming.dsl.Stream), which provides you with convenient combinators (e.g. `flatMap`, `groupByKey`) to easily write up transformations.

### IDE Setup (Optional)

You can get your preferred IDE ready for Gearpump by following [this guide](dev-ide-setup).

### Submit application

Finally, you need to package everything into a uber jar with [proper dependencies](http://gearpump.apache.org/downloads.html#maven-dependencies) and submit it to a Gearpump cluster. Please check out the [application submission tool](../introduction/commandline).




