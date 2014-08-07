#$SCALA_HOME/bin/scala -classpath conf:./lib/* -i shellenv.scala

$JAVA_HOME/bin/java -Xmx256M -Xms32M  -classpath conf:./lib/*:$SCALA_HOME/lib/akka-actors.jar:$SCALA_HOME/lib/jline.jar:$SCALA_HOME/lib/scala-actors.jar:$SCALA_HOME/lib/scala-actors-migration.jar:$SCALA_HOME/lib/scala-compiler.jar:$SCALA_HOME/lib/scala-library.jar:$SCALA_HOME/lib/scalap.jar:$SCALA_HOME/lib/scala-reflect.jar:$SCALA_HOME/lib/scala-swing.jar:$SCALA_HOME/lib/typesafe-config.jar -Dscala.home=$SCALA_HOME -Dscala.usejavacp=true scala.tools.nsc.MainGenericRunner  -i shellenv.scala



