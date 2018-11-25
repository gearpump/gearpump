SOL is a throughput test. It will create multiple layers, and then do random shuffling between these layers.

SOLProducer -> SOLProcessor -> SOLProcessor -> ...

The original code comes from: https://github.com/yahoo/storm-perf-test

<B>Supported configurations</B>
```
    "streamProducer"-> CLIOption[Int]("<stream producer number>", required = false, defaultValue = Some(2)),
    "streamProcessor"-> CLIOption[Int]("<stream processor number>", required = false, defaultValue = Some(2)),
    "bytesPerMessage" -> CLIOption[Int]("<size of each message>", required = false, defaultValue = Some(100)),
    "stages"-> CLIOption[Int]("<how many stages to run>", required = false, defaultValue = Some(2)))
```

<B>Example:</B>
```
bin/gear app -jar examples/gearpump-examples-assembly-$VERSION.jar io.gearpump.streaming.examples.sol.SOL
```
