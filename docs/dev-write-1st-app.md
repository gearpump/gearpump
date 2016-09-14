---
layout: global
displayTitle: Write Your 1st Gearpump App
title: Write Your 1st Gearpump App
description: Write Your 1st Gearpump App
---

We'll use [wordcount](https://github.com/apache/incubator-gearpump/tree/master/examples/streaming/wordcount/src/main/scala/org/apache/gearpump/streaming/examples/wordcount) as an example to illustrate how to write Gearpump applications.

### Maven/Sbt Settings

Repository and library dependencies can be found at [Maven Setting](maven-setting.html).

### IDE Setup (Optional)
You can get your preferred IDE ready for Gearpump by following [this guide](dev-ide-setup.html).

### Decide which language and API to use for writing 
Gearpump supports two level APIs:

1. Low level API, which is more similar to Akka programming, operating on each event. The API document can be found at [Low Level API Doc](http://gearpump.apache.org/releases/latest/api/scala/index.html#org.apache.gearpump.streaming.package).

2. High level API (aka DSL), which is operating on streaming instead of individual event. The API document can be found at [DSL API Doc](http://gearpump.apache.org/releases/latest/api/scala/index.html#org.apache.gearpump.streaming.dsl.package).

And both APIs have their Java version and Scala version.

So, before you writing your first Gearpump application, you need to decide which API to use and which language to use. 

## DSL version for Wordcount

The easiest way to write your streaming application is to write it with Gearpump DSL. 
Below will demostrate how to write WordCount application via Gearpump DSL.


<div class="codetabs">
<div data-lang="scala"  markdown="1" >


```scala
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

    val appId = context.submit(app)
    context.close()
  }
}
```

</div>

<div data-lang="java" markdown="1">

```java

/** Java version of WordCount with high level DSL API */
public class WordCount {

  public static void main(String[] args) throws InterruptedException {
    main(ClusterConfig.defaultConfig(), args);
  }

  public static void main(Config akkaConf, String[] args) throws InterruptedException {
    ClientContext context = new ClientContext(akkaConf);
    JavaStreamApp app = new JavaStreamApp("JavaDSL", context, UserConfig.empty());
    List<String> source = Lists.newArrayList("This is a good start, bingo!! bingo!!");

    //create a stream from the string list.
    JavaStream<String> sentence = app.source(source, 1, UserConfig.empty(), "source");

    //tokenize the strings and create a new stream
    JavaStream<String> words = sentence.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> apply(String s) {
        return Lists.newArrayList(s.split("\\s+")).iterator();
      }
    }, "flatMap");

    //map each string as (string, 1) pair
    JavaStream<Tuple2<String, Integer>> ones = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> apply(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    }, "map");

    //group by according to string
    JavaStream<Tuple2<String, Integer>> groupedOnes = ones.groupBy(new GroupByFunction<Tuple2<String, Integer>, String>() {
      @Override
      public String apply(Tuple2<String, Integer> tuple) {
        return tuple._1();
      }
    }, 1, "groupBy");

    //for each group, make the sum
    JavaStream<Tuple2<String, Integer>> wordcount = groupedOnes.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> apply(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
        return new Tuple2<String, Integer>(t1._1(), t1._2() + t2._2());
      }
    }, "reduce");

    //output result using log
    wordcount.log();

    app.run();
    context.close();
  }
}
```

</div>

</div>

## Low level API based Wordcount

### Define Processor(Task) class and Partitioner class

An application is a Directed Acyclic Graph (DAG) of processors. In the wordcount example, We will firstly define two processors `Split` and `Sum`, and then weave them together.


#### Split processor

In the `Split` processor, we simply split a predefined text (the content is simplified for conciseness) and send out each split word to `Sum`.

<div class="codetabs">
<div data-lang="scala"  markdown="1" >


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

</div>

<div data-lang="java" markdown="1">
```java
public class Split extends Task {

  public static String TEXT = "This is a good start for java! bingo! bingo! ";

  public Split(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
  }

  private Long now() {
    return System.currentTimeMillis();
  }

  @Override
  public void onStart(StartTime startTime) {
    self().tell(new Message("start", now()), self());
  }

  @Override
  public void onNext(Message msg) {

    // Split the TEXT to words
    String[] words = TEXT.split(" ");
    for (int i = 0; i < words.length; i++) {
      context.output(new Message(words[i], now()));
    }
    self().tell(new Message("next", now()), self());
  }
}
```

</div>

</div>

Essentially, each processor consists of two descriptions:

1. A `Task` to define the operation.

2. A parallelism level to define the number of tasks of this processor in parallel. 
 
Just like `Split`, every processor extends `Task`.  The `onStart` method is called once before any message comes in; `onNext` method is called to process every incoming message. Note that Gearpump employs the message-driven model and that's why Split sends itself a message at the end of `onStart` and `onNext` to trigger next message processing.

#### Sum Processor

The structure of `Sum` processor looks much alike. `Sum` does not need to send messages to itself since it receives messages from `Split`.

<div class="codetabs">
<div data-lang="scala"  markdown="1" >

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

</div>
<div data-lang="java" markdown="1">

```java
public class Sum extends Task {

  private Logger LOG = super.LOG();
  private HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

  public Sum(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
  }

  @Override
  public void onStart(StartTime startTime) {
    //skip
  }

  @Override
  public void onNext(Message messagePayLoad) {
    String word = (String) (messagePayLoad.msg());
    Integer current = wordCount.get(word);
    if (current == null) {
      current = 0;
    }
    Integer newCount = current + 1;
    wordCount.put(word, newCount);
  }
}
```

</div>

</div>

Besides counting the sum, in Scala version, we also define a scheduler to report throughput every 5 seconds. The scheduler should be cancelled when the computation completes, which could be accomplished overriding the `onStop` method. The default implementation of `onStop` is a no-op.

#### Partitioner

A processor could be parallelized to a list of tasks. A `Partitioner` defines how the data is shuffled among tasks of Split and Sum. Gearpump has already provided two partitioners

* `HashPartitioner`: partitions data based on the message's hashcode
* `ShufflePartitioner`: partitions data in a round-robin way.

You could define your own partitioner by extending the `Partitioner` trait/interface and overriding the `getPartition` method.


```scala
trait Partitioner extends Serializable {
  def getPartition(msg : Message, partitionNum : Int) : Int
}
```

### Wrap up as an application 

Now, we are able to write our application class, weaving the above components together.

The application class extends `App` and `ArgumentsParser which make it easier to parse arguments and run main functions.

<div class="codetabs">
<div data-lang="scala"  markdown="1" >

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

</div>

<div data-lang="java" markdown="1">

```java

/** Java version of WordCount with Processor Graph API */
public class WordCount {

  public static void main(String[] args) throws InterruptedException {
    main(ClusterConfig.defaultConfig(), args);
  }

  public static void main(Config akkaConf, String[] args) throws InterruptedException {

    // For split task, we config to create two tasks
    int splitTaskNumber = 2;
    Processor split = new Processor(Split.class).withParallelism(splitTaskNumber);

    // For sum task, we have two summer.
    int sumTaskNumber = 2;
    Processor sum = new Processor(Sum.class).withParallelism(sumTaskNumber);

    // construct the graph
    Graph graph = new Graph();
    graph.addVertex(split);
    graph.addVertex(sum);

    Partitioner partitioner = new HashPartitioner();
    graph.addEdge(split, partitioner, sum);

    UserConfig conf = UserConfig.empty();
    StreamApplication app = new StreamApplication("wordcountJava", conf, graph);

    // create master client
    // It will read the master settings under gearpump.cluster.masters
    ClientContext masterClient = new ClientContext(akkaConf);

    masterClient.submit(app);

    masterClient.close();
  }
}
```

</div>

</div>



## Submit application

After all these, you need to package everything into a uber jar and submit the jar to Gearpump Cluster. Please check [Application submission tool](commandline.html) to command line tool syntax.

## Advanced topic
For a real application, you definitely need to define your own customized message passing between processors.
Customized message needs customized serializer to help message passing over wire.
Check [this guide](dev-custom-serializer.html) for how to customize serializer.

### Gearpump for Non-Streaming Usage
Gearpump is also able to as a base platform to develop non-streaming applications. See [this guide](dev-non-streaming-example.html) on how to use Gearpump to develop a distributed shell.
