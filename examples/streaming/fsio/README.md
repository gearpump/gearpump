This directory contains the example that reads a hadoop sequence file and duplicate its content to new sequence files. This README explain how to quick-start this example.

First of all, this example is a simple case and here are some limitations you should know:

  1. The example only accepts one sequence file, not a directory, and the output file format is also sequence file.

  2. The example will duplicate the input file constantly, so if the example runs for a long time, the output files will be large.

  3. Each SeqFileStreamProcessor will generate a output file.

In order to run the example:

  1. Prepare a sequence file first, save it to local file system or HDFS.

  2. Start a gearpump cluster, including Master and Workers.

  3. Submit the application:<br>
  ```bash
  ./target/pack/bin/gear app -jar ./examples/target/$SCALA_VERSION_MAJOR/gearpump-examples-assembly-$VERSION.jar gearpump.streaming.examples.sol.SOL -input $INPUT_FILE_PATH -output $OUTPUT_DIRECTORY
  ```
  4. Stop the applcation:<br>
  ```bash
  ./target/pack/bin/gear kill -appid $APPID
  ```
  
  Note that the output parameter should be a directory.





