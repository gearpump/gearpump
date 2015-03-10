This directory contains the example that distributes a shell command to the cluster. This README explain how to quick-start this example.

This example also aims to explore better API for user to implement a new application, including AppMaster and Task.

In order to run the example:

  1. Start a gearpump cluster, including Master and Workers.

  2. Start the AppMaster:<br>
  ```bash
  target/pack/bin/gear app -jar experiments/distributedshell/target/$SCALA_VERSION_MAJOR/gearpump-experiments-distributedshell_$VERSION.jar org.apache.gearpump.distributedshell.DistributedShell -master 127.0.0.1:3000
  ```

  3. Submit the shell command:<br>
  ```bash
  target/pack/bin/gear app -jar experiments/distributedshell/target/$SCALA_VERSION_MAJOR/gearpump-experiments-distributedshell_$VERSION.jar org.apache.gearpump.distributedshell.DistributedShellClient -master 127.0.0.1:3000 -appid $APPID -command "ls /"
  ```