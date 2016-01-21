// Check http://www.scala-sbt.org/0.13/docs/Parallel-Execution.html
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

// http://www.scala-sbt.org/0.13/docs/Testing.html
logBuffered in Test := false

