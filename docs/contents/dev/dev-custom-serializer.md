Gearpump has a built-in serialization framework with a shaded Kryo version, which allows you to customize how a specific message type can be serialized. 

#### Register a class before serialization.

Note, to use built-in kryo serialization framework, Gearpump requires all classes to be registered explicitly before using, no matter you want to use a custom serializer or not. If not using custom serializer, Gearpump will use default com.esotericsoftware.kryo.serializers.FieldSerializer to serialize the class. 

To register a class, you need to change the configuration file gear.conf(or application.conf if you want it only take effect for single application).

	:::json
	gearpump {
	  serializers {
	    ## We will use default FieldSerializer to serialize this class type
	    "io.gearpump.UserMessage" = ""
	    
	    ## we will use custom serializer to serialize this class type
	    "io.gearpump.UserMessage2" = "io.gearpump.UserMessageSerializer"
	  }
	}
	

#### How to define a custom serializer for built-in kryo serialization framework

When you decide that you want to define a custom serializer, you can do this in two ways.

Please note that Gearpump shaded the original Kryo dependency. The package name ```com.esotericsoftware``` was relocated to ```io.gearpump.esotericsoftware```. So in the following customization, you should import corresponding shaded classes, the example code will show that part.

In general you should use the shaded version of a library whenever possible in order to avoid binary incompatibilities, eg don't use:

	:::scala
	import com.google.common.io.Files


but rather

	:::scala
	import io.gearpump.google.common.io.Files


##### System Level Serializer

If the serializer is widely used, you can define a global serializer which is available to all applications(or worker or master) in the system.

###### Step1: you first need to develop a java library which contains the custom serializer class. here is an example:

	:::scala
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


###### Step2: Distribute the libraries

Distribute the jar file to lib/ folder of every Gearpump installation in the cluster.

###### Step3: change gear.conf on every machine of the cluster:

	:::json
	gearpump {
	  serializers {
	    "io.gearpump.UserMessage" = "io.gearpump.UserMessageSerializer"
	  }
	}
	

###### All set!

##### Define Application level custom serializer
If all you want is to define an application level serializer, which is only visible to current application AppMaster and Executors(including tasks), you can follow a different approach.

###### Step1: Define your custom Serializer class

You should include the Serializer class in your application jar. Here is an example to define a custom serializer:

	:::scala
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


###### Step2: Put a application.conf in your classpath on Client machine where you submit the application, 

	:::json
	### content of application.conf
	gearpump {
	  serializers {
	    "io.gearpump.UserMessage" = "io.gearpump.UserMessageSerializer"
	  }
	}
	

###### Step3: All set!

#### Advanced: Choose another serialization framework

Note: This is only for advanced user which require deep customization of Gearpump platform.

There are other serialization framework besides Kryo, like Protobuf. If user don't want to use the built-in kryo serialization framework, he can customize a new serialization framework. 

basically, user need to define in gear.conf(or application.conf for single application's scope) file like this:

	:::bash
	gearpump.serialization-framework = "io.gearpump.serializer.CustomSerializationFramework"
	

Please find an example in gearpump storm module, search "StormSerializationFramework" in source code.