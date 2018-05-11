# MirrorTool for Kafka Connect

A Kafka Source Connector for [Kafka Connect](https://kafka.apache.org/documentation/#connect). Mirror topics from a source Kafka cluster into your destination Kafka cluster.

## Why

The aim of this connector is to enable [MirrorMaker](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330)-like functionality within the Kafka Connect framework.

At Comcast, we have seen two key benefits to using MirrorTool over MirrorMaker:

* No access to the ZooKeeper on the source kafka clusters required.
* Can use existing Kafka Connect deployment, instead of having to manage and monitor another stand alone application

![High level diagram of MirrorTool. Source Kafka cluster on the left of the image, with arrow indicating data flow from the source Kafka cluster to the MirrorTool connector/Kafka Connect  in the center of image. Dashed line from MirrorTool to source Kafka cluster indicating monitoring of the source Kafka cluster for topics and partitions to mirror. Arrow indicating data flow from Kafka Connect to the destination Kafka cluster on the right of the image.](doc/HLDiagram.png?raw=true "High Level Diagram of MirrorTool")

## How it works

* Connector starts up a "Partition Monitor" thread which uses the Kafka AdminClient to get a list of topics (and partitons) from the source cluster to be mirrored to the destination cluster.
* List of available topic partitions are allocated to the configured number of tasks.
* Each task starts a KafkaConsumer and assigns its allocated topic partitions to the consumer.
* As messages are consumed from the source cluster, they are passed to the Kafka Conenct framework to deliver the messages to the destination kafka cluster
* The "Partition Monitor" thread periodically checks the source cluster for changes to the topics and partitions that have been configured for the connector, and will trigger a task reconfiguration if required.

## Build

Output a fat jar to `build/libs/MirrorTool-[version]-all.jar` by running:

`gradle shadowJar`

Copy the resulting jar file into your Kafka Connect plugin directory.

## Configuration Options

Note that these options are for this MirrorTool connector, and assumes some familiarity with the base Kafka Connect configuration options. (You will need to configure the parameters for your destination Kafka Cluster in the base Kafka Connect configuration)

## Common Options

These are the most common options that are required when configuring this connector:

Configuration Parameter | Example | Description
----------------------- | ------- | -----------
**consumer.bootstrap.servers** | source.broker1:9092,source.broker2:9092 | **Mandatory.** Comma separated list of boostrap servers for the source Kafka cluster
**consumer.auto.offset.reset** | latest | If there is no stored offset for a partition, indicates where to start consuming from. Options are _"earliest"_ or _"latest"_. Default: _earliest_
**source.topics** | an.interesting.topic,another.topic | Comma separated list of topics to mirror (Note: Cannot be used with _source.topics.regex_)
**source.topics.regex** | my.topics.* | Java regular expression to match topics to mirror (Note: Cannot be used with _source.topics.regex_)
**destination.topics.prefix** | aggregate. | Prefix to add to source topic names when determining the Kafka topic to publish data to

## Advanced Options

Some use cases may require modifying the following connector options. Use with care.

Configuration Parameter | Default | Description
----------------------- | ------- | -----------
**include.message.headers** | true | Indicates whether message headers from source records should be included when delivered to the destination cluster.
**poll.timeout.ms** | 1000 | Maximum amount of time (in milliseconds) the connector will wait in each poll loop without data before returning control to the kafka connect task thread.
**max.shutdown.wait.ms** | 1500 | Maximum amount of time (in milliseconds) to wait for the connector to gracefully shut down before forcing the consumer and admin clients to close. Note that any values greater than the kafka connect parameter *task.shutdown.graceful.timeout.ms* will not have any effect.
**consumer.max.poll.records** | 500 | Maximum number of records to return from each poll of the internal KafkaConsumer. When dealing with topics with very large messages, the connector may sometimes spend too long processing each batch of records, causing lag in offset commits, or in serious cases, unnecessary consumer rebalances. Reducing this value can help in these scenarios. Conversely, when processing very small messages, increasing this value may improve overall throughput.
**partition.monitor.topic.list.timeout.ms** | 60000 | Amount of time (in milliseconds) the partition monitor thread should wait for the source kafka cluster to return topic information before logging a timeout error.
**partition.monitor.poll.interval.ms** | 300000 | Amount of time (in milliseconds) the partition monitor will wait before re-querying the source cluster for a change in the partitions to be consumed
**partition.monitor.reconfigure.tasks.on.leader.change** | false | Indicates whether the partition monitor should request a task reconfiguration when partition leaders have changed. In some cases this may be a minor optimization as when generating task configurations, the connector will try to group partitions to be consumed by each task by the leader node. The downside to this is that it may result in additional rebalances.

### Overriding the internal KafkaConsumer Configuration
Note that standard Kafka parameters can be passed to the internal KafkaConsumer and AdminClient by prefixing the standard consumer configuration parameters with "**consumer.**". This is especially useful in cases where you need to pass authentication parameters to these clients.

### Example Configuration

```javascript
{
  "name": "mirrortool-example", // Name of the kafka connect task
    "config": {
    "tasks.max": "4", // Maximum number of parallel tasks to run
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", // Set the format of data that is written to kafka. As the connector parses source cluster messages as binary arrays, this will be the usual required value here
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", // Set the format of data that is written to kafka. As the connector parses source cluster messages as binary arrays, this will be the usual required value here
    "connector.class": "com.comcast.kafka.connect.kafka.KafkaSourceConnector", // The full class name of this connector
    "consumer.bootstrap.servers": "kafka.bootstrap.server1:9092,kafka.bootstrap.server2:9093", // Kafka boostrap servers for source cluster
    "source.topics": "customer.orders,customer.feedback", // Mirror these two topics from the source cluster
    "destination.topics.prefix": "aggregate.", // Add "aggregate." as a prefix to the original topic name when sending to the destination cluster
    "consumer.auto.offset.reset": "latest", // Start from the latest offsets if no offsets are stored in kafka connect's offset store
  }
}
```
