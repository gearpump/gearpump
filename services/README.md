## gearpump-dashboard

> Dashboard framework with Angular.js, Twitter Bootstrap and Font Awesome, d3.

## Getting started

Build Gearpump and run:

```bash
cd ~/gearpump
sbt clean publishLocal assembly pack
target/pack/bin/local
target/pack/bin/services
target/pack/bin/gear app -jar ./examples/complexdag/target/scala-2.11/gepump-examples-complexdag_2.11-0.2.4-SNAPSHOT.jar io.gearpump.streaming.examples.complexdag.Dag
```

Launch your browser at http://localhost:8090

