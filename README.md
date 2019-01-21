# MirrorTool for Kafka Connect

A Kafka Source Connector for [Kafka Connect](https://kafka.apache.org/documentation/#connect). Mirror topics from a source Kafka cluster into your destination Kafka cluster.

## Downloads

You can grab a pre-built jar on the [Releases](https://github.com/Comcast/MirrorTool-for-Kafka-Connect/releases) page.

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

Build a plugin jar to `build/libs/MirrorTool-[version].jar` by running:

`./gradlew build`

Copy the resulting jar file into your Kafka Connect plugin directory.

## Configuration Options

Note that these options are for this MirrorTool connector, and assumes some familiarity with the base Kafka Connect configuration options. (You will need to configure the parameters for your destination Kafka Cluster in the base Kafka Connect configuration)

## Common Options

These are the most common options that are required when configuring this connector:

Configuration Parameter | Example | Description
----------------------- | ------- | -----------
**source.bootstrap.servers** | source.broker1:9092,source.broker2:9092 | **Mandatory.** Comma separated list of boostrap servers for the source Kafka cluster
**source.topic.whitelist** | topic, topic-prefix* | Java regular expression to match topics to mirror. For convenience, comma (',') is interpreted as the regex-choice symbol ('|').
**source.auto.offset.reset** | earliest | If there is no stored offset* for a partition, indicates where to start consuming from. Options are _"earliest"_,  _"latest"_, or _"none"_. Default: _earliest_
**source.group.id** | kafka-connect | Group ID used when writing offsets back to source cluster (for offset lag tracking)

* "*Stored offset*" here does not mean the stored consumer group offset, rather the stored offset within the Kafka Connect `offset.storage.topic` topic. If you want to start the connector from an existing consumer group, then set `source.auto.offset.reset` to `none`, and update `source.group.id` accordingly.

## Advanced Options

Some use cases may require modifying the following default connector options. Use with care.

Configuration Parameter | Default | Description
----------------------- | ------- | -----------
**include.message.headers** | true | Indicates whether message headers from source records should be included when delivered to the destination cluster.
**topic.list.timeout.ms** | 60000 | Amount of time (in milliseconds) the partition monitor thread should wait for the source kafka cluster to return topic information before logging a timeout error.
**topic.list.poll.interval.ms** | 300000 | Amount of time (in milliseconds) the partition monitor will wait before re-querying the source cluster for a change in the topic partitions to be consumed
**reconfigure.tasks.on.leader.change** | false | Indicates whether the partition monitor should request a task reconfiguration when partition leaders have changed. In some cases this may be a minor optimization as when generating task configurations, the connector will try to group partitions to be consumed by each task by the leader node. The downside to this is that it may result in excessive rebalances.
**poll.loop.timeout.ms** | 1000 | Maximum amount of time (in milliseconds) the connector will wait in each poll loop without data before returning control to the kafka connect task thread.
**max.shutdown.wait.ms** | 2000 | Maximum amount of time (in milliseconds) to wait for the connector to gracefully shut down before forcing the consumer and admin clients to close. Note that any values greater than the kafka connect parameter *task.shutdown.graceful.timeout.ms* will not have any effect.
**source.max.poll.records** | 500 | Maximum number of records to return from each poll of the internal KafkaConsumer. When dealing with topics with very large messages, the connector may sometimes spend too long processing each batch of records, causing lag in offset commits, or in serious cases, unnecessary consumer rebalances. Reducing this value can help in these scenarios. Conversely, when processing very small messages, increasing this value may improve overall throughput.
**source.key.deserializer** | org.apache.kafka.common.serialization.ByteArrayDeserializer | Key deserializer to use for the kafka consumers connecting to the source cluster.
**source.value.deserializer** | org.apache.kafka.common.serialization.ByteArrayDeserializer | Value deserializer to use for the kafka consumers connecting to the source cluster.
**source.enable.auto.commit** | true | If true the consumer's offset will be periodically committed to the source cluster in the background.
Note that these offsets are not used to resume the connector (They are stored in the Kafka Connect offset store), but may be useful in monitoring the current offset lag of this connector on the source cluster

### Overriding the internal KafkaConsumer and AdminClient Configuration

Note that standard Kafka parameters can be passed to the internal KafkaConsumer and AdminClient by prefixing the standard configuration parameters with "*source.*".
For cases where the configuration for the KafkaConsumer and AdminClient diverges, you can use the more explicit "*connector.consumer.*" and "*connector.admin.*" configuration parameter prefixes to fine tune the settings used for each.

### Example Configuration

```javascript
{
  "name": "kafka-connect-kafka-source-example", // Name of the kafka connect task
    "config": {
    "tasks.max": "2", // Maximum number of parallel tasks to run
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", // Set the format of data that is written to kafka. As the connector parses source cluster messages as binary arrays, this will be the usual required value here
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", // Set the format of data that is written to kafka. As the connector parses source cluster messages as binary arrays, this will be the usual required value here
    "connector.class": "com.comcast.kafka.connect.kafka.KafkaSourceConnector", // The full class name of this connector
    "source.bootstrap.servers": "kafka.bootstrap.server1:9092,kafka.bootstrap.server2:9093", // Kafka boostrap servers for source cluster
    "source.topic.whitelist": "test.topic.*", // Mirror topics matching this regex
    "source.auto.offset.reset": "earliest", // For partitions without existing offsets stored, start at the head of the partition
    "source.group.id": "kafka-connect-testing", // Use this group ID when commiting offsets to the source cluster
    "connector.consumer.reconnect.backoff.max.ms": "10000" // Override the default consumer setting "reconnect.backoff.max.ms"
  }
}
```
