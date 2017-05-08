# Gearpump RabbitMQ

Gearpump integration for [RabbitMQ](https://www.rabbitmq.com/)

## Usage

The message type that RMQSink is able to handle including:

 1. String
 2. Array[Byte]
 3. Sequence of type 1 and 2

Suppose there is a DataSource Task will output above-mentioned messages, you can write a simple application then:

```scala
val sink = new RMQSink(UserConfig.empty)
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
val split = Processor[DataSource]("$splitNum")
val computation = split ~> sinkProcessor
val application = StreamApplication("RabbitMQ", Graph(computation), UserConfig.empty)
```
## config items
to initialize the RMQSink's instance, we need a UserConfig object and should provide some config item list below :

* [must]`rabbitmq.queue.name` : the RabbitMQ queue name we want to sink the message to;
* [optional]`rabbitmq.connection.host` : the RabbitMQ server host;
* [optional]`rabbitmq.connection.port` : the RabbitMQ server port, default port is **5672**;
* [optional]`rabbitmq.connection.uri` : the connection uri, pattern is `amqp://userName:password@hostName:portNumber/virtualHost`
* [optional]`rabbitmq.virtualhost` : the virtual-host which is a logic domain in RabbitMQ Server
* [optional]`rabbitmq.auth.username` : the user name for authorization
* [optional]`rabbitmq.auth.password` : the password for authorization
* [optional]`rabbitmq.automatic.recovery` : if need automatic recovery set `true` otherwise set `false`
* [optional]`rabbitmq.connection.timeout` : the connection's timeout
* [optional]`rabbitmq.network.recovery.internal` : recovery internal
* [optional]`rabbitmq.requested.heartbeat` : if need heartbeat set `true` otherwise set `false`
* [optional]`rabbitmq.topology.recoveryenabled` : if need recovery set `true` otherwise set `false`
* [optional]`rabbitmq.channel.max` : the maximum channel num
* [optional]`rabbitmq.frame.max` : the maximum frame num

more details : https://www.rabbitmq.com/admin-guide.html