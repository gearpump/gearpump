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

Security: this command-execution example is opt-in and excluded from the default
aggregate/distribution. Build explicitly with
`sbt gearpump-examples-distributedshell/assembly`; the JAR is under
`examples/distributedshell/target/opt-in`. Use only in an isolated worker account
or container approved for command execution. Commands intentionally invoke
processes under that account; application authorization is not an OS sandbox.

The client must authenticate to the master as a cluster administrator to retrieve
an application-scoped capability. Both the AppMaster and each ShellExecutor check
the capability and application ID before running a command. Raw `ShellCommand`
messages and another application's capability are rejected. Upgrade all peers
with the cluster capability and worker allocation PRs before deploying.
