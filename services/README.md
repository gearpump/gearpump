## gearpump-dashboard

> Dashboard framework with Angular.js, Twitter Bootstrap and Font Awesome, d3.

## Getting started

Install bower:

```bash
sudo npm install -g bower
```

Install bower dependencies:

```bash
bower install
```

Build GearPump and run:

```bash
cd ~/gearpump
sbt clean publishLocal pack
target/pack/bin/local -ip 127.0.0.1 -port 3000
target/pack/bin/services -master 127.0.0.1:3000
target/pack/bin/gear app -jar ./examples/complexdag/target/scala-2.11/gepump-examples-complexdag_2.11-0.2.4-SNAPSHOT.jar org.apache.gearpump.streaming.examples.complexdag.Dag -master 127.0.0.1:3000
```

Launch your browser at http://localhost:8090

