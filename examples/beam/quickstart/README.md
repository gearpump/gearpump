# Gearpump Beam Quick Start

This example submits a small Apache Beam pipeline to a Gearpump local cluster. By default, the
bounded pipeline tokenizes two lines, counts words with `Sum.integersPerKey()`, and writes the
result to `target/beam-quickstart/output.txt`.

From a source checkout, use a stable runtime classpath for long-running local daemons:

```bash
mkdir -p target/beam-quickstart
cp conf/gear.conf target/beam-quickstart/gear.conf
perl -0pi -e \
  's/masters = \["127\.0\.0\.1:3000"\]/masters = ["127.0.0.1:3010"]/g' \
  target/beam-quickstart/gear.conf
printf '\ngearpump.worker.executor-share-same-jvm-as-worker = true\n' \
  >> target/beam-quickstart/gear.conf

sbt "show gearpump-examples-beam-quickstart / Runtime / fullClasspath" \
  > target/beam-quickstart/fullclasspath.raw
sed -n 's/^\[info\] \* Attributed(\(.*\))$/\1/p' \
  target/beam-quickstart/fullclasspath.raw \
  > target/beam-quickstart/classpath.entries
paste -sd: target/beam-quickstart/classpath.entries \
  > target/beam-quickstart/classpath.txt
```

Start the local master and at least one worker in separate shells:

```bash
java -Djava.net.preferIPv4Stack=true \
  -Dgearpump.home="$PWD" \
  -Dgearpump.config.file=target/beam-quickstart/gear.conf \
  -cp "conf:$(cat target/beam-quickstart/classpath.txt)" \
  io.gearpump.cluster.main.Master -ip 127.0.0.1 -port 3010

java -Djava.net.preferIPv4Stack=true \
  -Dgearpump.home="$PWD" \
  -Dgearpump.config.file=target/beam-quickstart/gear.conf \
  -cp "conf:$(cat target/beam-quickstart/classpath.txt)" \
  io.gearpump.cluster.main.Worker
```

Submit the Beam pipeline:

```bash
java -Djava.net.preferIPv4Stack=true \
  -Dgearpump.home="$PWD" \
  -Dgearpump.config.file=target/beam-quickstart/gear.conf \
  -cp "conf:$(cat target/beam-quickstart/classpath.txt)" \
  io.gearpump.examples.beam.quickstart.BeamQuickStart \
  --remote=true \
  --output=target/beam-quickstart/output.txt
```

The client waits for the bounded output, terminates the application, and prints the sorted counts:

```text
apache=1
beam=2
gearpump=2
on=1
pipelines=1
runs=2
```

To keep the application visible as active in the Web UI after the bounded output is written, submit
with `--keepRunning=true`:

```bash
java -Djava.net.preferIPv4Stack=true \
  -Dgearpump.home="$PWD" \
  -Dgearpump.config.file=target/beam-quickstart/gear.conf \
  -cp "conf:$(cat target/beam-quickstart/classpath.txt)" \
  io.gearpump.examples.beam.quickstart.BeamQuickStart \
  --remote=true \
  --output=target/beam-quickstart/output.txt \
  --keepRunning=true
```

Use `--keepRunningMillis=<milliseconds>` with `--keepRunning=true` to stop after a fixed delay
instead of waiting until the client is interrupted. Long-running mode adds a basic unbounded Beam
source that writes a periodic heartbeat to `<output>.keep-running` while the application stays
active.
