This example demonstrates how to write messages into a Parquet format file.

### How to run the example
  1. ```sbt avro:generate```<br>
  This command will compile the .avsc file to a avro .java file.
   
  2. ```sbt assembly```<br>
  This command will generate a jar file under experiments/parquet/target/scala-2.11
  
  3. ```bin/gear app -jar jarfile -output file:///tmp/parquet```<br>
  Submit the application, which will generate parquet files under folder /tmp/parquet