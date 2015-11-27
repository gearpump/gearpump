---
layout: global
title: Gearpump Customize Serializer
---
#### Define Custom Message Serializer

We use library [kryo](https://github.com/EsotericSoftware/kryo) and [akka-kryo library](https://github.com/romix/akka-kryo-serialization). If you have special Message type, you can choose to define your own serializer explicitly. If you have not defined your own custom serializer, the system will use Kryo to serialize it at best effort.

When you have determined that you want to define a custom serializer, you can do this in two ways.

Please note that Gearpump shaded the original Kryo dependency. The package name ```com.esotericsoftware``` was relocated to ```io.gearpump.esotericsoftware```. So in the following customization, you should import corresponding shaded classes, the example code will show that part.

##### System Level Serializer

If the serializer is widely used, you can define a global serializer which is avaiable to all applications(or worker or master) in the system.

###### Step1: you first need to develop a java library which contains the custom serializer class. here is an example:

```scala
package io.gearpump

import io.gearpump.esotericsoftware.kryo.{Kryo, Serializer}
import io.gearpump.esotericsoftware.kryo.io.{Input, Output}

class UserMessage(longField: Long, intField: Int)

class UserMessageSerializer extends Serializer[UserMessage] {
  override def write(kryo: Kryo, output: Output, obj: UserMessage) = {
    output.writeLong(obj.longField)
    output.writeInt(obj.intField)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[UserMessage]): UserMessage = {
    val longField = input.readLong()
    val intField = input.readInt()
    new UserMessage(longField, intField)
  }
}
```

###### Step2: Distribute the libraries

Distribute the jar file to lib/ folder of every Gearpump installation in the cluster.

###### Step3: change gear.conf on every machine of the cluster:

```
gearpump {
  serializers {
    "io.gearpump.UserMessage" = "io.gearpump.UserMessageSerializer"
  }
}
```

##### All set!

#### Define Application level custom serializer
If all you want is to define an application level serializer, which is only visible to current application AppMaster and Executors(including tasks), you can follow a different approach.

###### Step1: Define your custom Serializer class

You should include the Serializer class in your application jar. Here is an example to define a custom serializer:

```scala
package io.gearpump

import io.gearpump.esotericsoftware.kryo.{Kryo, Serializer}
import io.gearpump.esotericsoftware.kryo.io.{Input, Output}

class UserMessage(longField: Long, intField: Int)

class UserMessageSerializer extends Serializer[UserMessage] {
  override def write(kryo: Kryo, output: Output, obj: UserMessage) = {
    output.writeLong(obj.longField)
    output.writeInt(obj.intField)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[UserMessage]): UserMessage = {
    val longField = input.readLong()
    val intField = input.readInt()
    new UserMessage(longField, intField)
  }
}
```

###### Step2: Define a config file to include the custom serializer definition. For example, we can create a file called: myconf.conf


```
### content of myconf.conf
gearpump {
  serializers {
    "io.gearpump.UserMessage" = "io.gearpump.UserMessageSerializer"
  }
}
```

###### Step3: Add the conf into AppDescription

Let's take WordCount as an example:

```scala
object WordCount extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val RUN_FOR_EVER = -1

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(4)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(4)),
    "runseconds"-> CLIOption[Int]("<how long to run this example, set to -1 if run forever>", required = false, defaultValue = Some(60))
  )

  def application(config: ParseResult) : AppDescription = {
    val splitNum = config.getInt("split")
    val sumNum = config.getInt("sum")
    val partitioner = new HashPartitioner()
    val split = TaskDescription(classOf[Split].getName, splitNum)
    val sum = TaskDescription(classOf[Sum].getName, sumNum)

    //=======================================
    // Attention!
    //=======================================
    val app = AppDescription("wordCount", UserConfig.empty, Graph(split ~ partitioner ~> sum),
      ClusterConfigSource("/path/to/myconf.conf"))

    app
  }

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  implicit val system = context.system
  val appId = context.submit(application(config))
  Thread.sleep(config.getInt("runseconds") * 1000)
  context.shutdown(appId)
  context.close()
}

```

Maybe you have noticed, we have add a custom config to the Application

```scala
//=======================================
    // Attention!
    //=======================================
    val app = AppDescription("wordCount", UserConfig.empty, Graph(split ~ partitioner ~> sum),
      ClusterConfigSource("/path/to/myconf.conf"))
```

###### Step4: All set!
