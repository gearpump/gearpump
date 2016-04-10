A very basic DSL which support flatmap, reduce, and etc..
======================


Supported operators:
------------------
```scala
class Stream[T](dag: Graph[Op, OpEdge], private val thisNode: Op, private val edge: Option[OpEdge] = None) {

  /**
   * convert a value[T] to a list of value[R]
   */
  def flatMap[R](fun: T => TraversableOnce[R]): Stream[R]

  /**
   * convert value[T] to value[R]
   */
  def map[R](fun: T => R): Stream[R]

  /**
   * reserve records when fun(T) == true
   * @param fun
   * @return
   */
  def filter(fun: T => Boolean): Stream[T]

  /**
   * Reduce opeartion
   * @param fun
   * @return
   */
  def reduce(fun: (T, T) => T): Stream[T]

  /**
   * Log to task log file
   */
  def log(): Unit

  /**
   * Merge data from two stream into one
   * @param other
   * @return
   */
  def merge(other: Stream[T]): Stream[T]

  /**
   * Group by fun(T)
   *
   * For example, we have T type, People(name: String, gender: String, age: Int)
   * groupBy[People](_.gender) will group the people by gender.
   *
   * You can append other combinators after groupBy
   *
   * For example,
   *
   * Stream[People].groupBy(_.gender).flatmap(..).filter.(..).reduce(..)
   */
  def groupBy[Group](fun: T => Group, parallism: Int = 1): Stream[T]

  /**
   * connect with a low level Processor(TaskDescription)
   */
  def process[R](processor: Class[_ <: Task], parallism: Int): Stream[R]
}

```

How to define the DSL
---------------
WordCount:

```scala
val context = ClientContext(master)
    val app = StreamApp("dsl", context)

    val data = "This is a good start, bingo!! bingo!!"
    app.fromCollection(data.lines.toList).
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupBy(kv => kv._1).reduce((left, right) => (left._1, left._2 + right._2))

    val appId = context.submit(app)
    context.close()
```

For the full example, please check https://github.com/intel-hadoop/gearpump/tree/master/experiments/dsl/src/main/scala/io.gearpump/streaming/dsl/example


Run an example
---------------------
```bash
# start master
bin\local

# start UI
bin\services

# start example topology
bin\gear io.gearpump.streaming.dsl.example.WordCount
```