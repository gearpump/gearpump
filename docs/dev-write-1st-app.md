---
layout: global
displayTitle: Write Your 1st Gearpump App
title: Write Your 1st Gearpump App
description: Write Your 1st Gearpump App
---

We'll use [wordcount](https://github.com/gearpump/gearpump/blob/master/examples/streaming/wordcount/src/main/scala/io/gearpump/streaming/examples/wordcount/) as an example to illustrate how to write GearPump applications.

### Maven/Sbt Settings

Repository and library dependencies can be found at [Maven Setting](maven-setting.html).

### IDE Setup (Optional)
You can get your preferred IDE ready for Gearpump by following [this guide](dev-ide-setup.html).

### Define Processor(Task) class and Partitioner class

An application is a Directed Acyclic Graph (DAG) of processors. In the wordcount example, We will firstly define two processors `Split` and `Sum`, and then weave them together.

#### About message type

User are allowed to send message of type AnyRef(map to Object in java).

```
case class Message(msg: AnyRef, timestamp: TimeStamp = Message.noTimeStamp)
```

If user want to send primitive types like Int, Long, then he should box it explicitly with asInstanceOf. For example:

```
new Message(3.asInstanceOf[AnyRef])
```

#### Split processor

In the Split processor, we simply split a predefined text (the content is simplified for conciseness) and send out each split word to Sum.

Scala:

```scala
class Split(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.output

  override def onStart(startTime : StartTime) : Unit = {
    self ! Message("start")
  }

  override def onNext(msg : Message) : Unit = {
    Split.TEXT_TO_SPLIT.lines.foreach { line =>
      line.split("[\\s]+").filter(_.nonEmpty).foreach { msg =>
        output(new Message(msg, System.currentTimeMillis()))
      }
    }
    self ! Message("continue", System.currentTimeMillis())
  }
}

object Split {
  val TEXT_TO_SPLIT = "some text"
}
```



Like Split, every processor extends a `TaskActor`.  The `onStart` method is called once before any message comes in; `onNext` method is called to process every incoming message. Note that GearPump employs the message-driven model and that's why Split sends itself a message at the end of `onStart` and `onNext` to trigger next message processing.

#### Sum Processor

The structure of Sum processor looks much alike. Sum does not need to send messages to itself since it receives messages from Split.

Scala:

```scala
class Sum (taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private[wordcount] val map : mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  private[wordcount] var wordCount : Long = 0
  private var snapShotTime : Long = System.currentTimeMillis()
  private var snapShotWordCount : Long = 0

  private var scheduler : Cancellable = null

  override def onStart(startTime : StartTime) : Unit = {
    scheduler = taskContext.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportWordCount)
  }

  override def onNext(msg : Message) : Unit = {
    if (null == msg) {
      return
    }
    val current = map.getOrElse(msg.msg.asInstanceOf[String], 0L)
    wordCount += 1
    map.put(msg.msg.asInstanceOf[String], current + 1)
  }

  override def onStop() : Unit = {
    if (scheduler != null) {
      scheduler.cancel()
    }
  }

  def reportWordCount() : Unit = {
    val current : Long = System.currentTimeMillis()
    LOG.info(s"Task ${taskContext.taskId} Throughput: ${(wordCount - snapShotWordCount, (current - snapShotTime) / 1000)} (words, second)")
    snapShotWordCount = wordCount
    snapShotTime = current
  }
}
```

Besides counting the sum, we also define a scheduler to report throughput every 5 seconds. The scheduler should be cancelled when the computation completes, which could be accomplished overriding the `onStop` method. The default implementation of `onStop` is a no-op.

#### Partitioner

A processor could be parallelized to a list of tasks. A `Partitioner` defines how the data is shuffled among tasks of Split and Sum. GearPump has already provided two partitioners

* `HashPartitioner`: partitions data based on the message's hashcode
* `ShufflePartitioner`: partitions data in a round-robin way.

You could define your own partitioner by extending the `Partitioner` trait and overriding the `getPartition` method.

```scala
trait Partitioner extends Serializable {
  def getPartition(msg : Message, partitionNum : Int) : Int
}
```

### Define TaskDescription and AppDescription

Now, we are able to write our application class, weaving the above components together.

The application class extends `App` and `ArgumentsParser which make it easier to parse arguments and run main functions.

```scala
object WordCount extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val RUN_FOR_EVER = -1

  override val options: Array[(String, CLIOption[Any])] = Array(
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(1)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1))
  )

  def application(config: ParseResult) : StreamApplication = {
    val splitNum = config.getInt("split")
    val sumNum = config.getInt("sum")
    val partitioner = new HashPartitioner()
    val split = Processor[Split](splitNum)
    val sum = Processor[Sum](sumNum)
    val app = StreamApplication("wordCount", Graph[Processor[_ <: Task], Partitioner](split ~ partitioner ~> sum), UserConfig.empty)
    app
  }

  val config = parse(args)
  val context = ClientContext()
  val appId = context.submit(application(config))
  context.close()
}

```

We override `options` value and define an array of command line arguments to parse. We want application users to pass in masters' hosts and ports, the parallelism of split and sum tasks, and how long to run the example. We also specify whether an option is `required` and provide `defaultValue` for some arguments.

Given the `ParseResult` of command line arguments, we create `TaskDescription`s for Split and Sum processors, and connect them with `HashPartitioner` using DAG API. The graph is wrapped in an `AppDescrition` , which is finally submit to master.

### Submit application

After all these, you need to package everything into a uber jar and submit the jar to Gearpump Cluster. Please check [Application submission tool](Commandline.html) to command line tool syntax.


### Advanced topic
For a real application, you definitely need to define your own customized message passing between processors.
Customized message needs customized serializer to help message passing over wire.
Check [this guide](dev-connectors.html) for how to customize serializer.

### Gearpump for Non-Streaming Usage
Gearpump is also able to as a base platform to develop non-streaming applications. See [this guide](dev-non-streaming-example.html) on how to use Gearpump to develop a distributed shell.
