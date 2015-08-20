### How to run the page demo code

After compile,
```scala
bin\gear io.gearpump.experiments.pagerank.example.PageRankExample
```

### Syntax

```scala
  val a = "a"
  val b = "b"
  val c = "c"
  val d = "d"

  val pageRankGraph = Graph(a ~> b, a~>c, a~>d, b~>a, b~>d, d~>b, d~>c, c~>b)

  val app = new PageRankApplication("pagerank", iteration = 100, delta = 0.001, pageRankGraph)

  val context = ClientContext()

  val appId = context.submit(app)

  context.close()
```