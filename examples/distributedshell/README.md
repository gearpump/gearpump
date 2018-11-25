This directory contains the example that distributes a shell command to the cluster. This README explain how to quick-start this example.

This example also aims to explore better API for user to implement a new application, including AppMaster and Task.

In order to run the example:

  1. Start a gearpump cluster, including Master and Workers.

  2. Start the AppMaster:<br>
  ```bash
  target/pack/bin/gear app -jar target/pack/examples/gearpump-experiments-distributedshell_$VERSION.jar io.gearpump.examples.distributedshell.DistributedShell
  ```

  3. Submit the shell command:<br>
  ```bash
  target/pack/bin/gear app -verbose true -jar target/pack/examples/gearpump-experiments-distributedshell_$VERSION.jar io.gearpump.examples.distributedshell.DistributedShellClient -appid $APPID -command "ls /"
  ```