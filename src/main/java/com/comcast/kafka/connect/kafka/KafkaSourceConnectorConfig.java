/**
 * Copyright 2018 Comcast Cable Communications Management, LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); * you may not use this file except in compliance with the License. * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.comcast.kafka.connect.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.*;

public class KafkaSourceConnectorConfig extends AbstractConfig {

    // Config Prefixes
    public static final String CONSUMER_PREFIX =            "consumer.";
    public static final String PARTITION_MONITOR_PREFIX =   "partition.monitor.";
    public static final String TASK_PREFIX =                "task.";

    // Topic partition list we send to each task. Not user configurable.
    public static final String TASK_LEADER_TOPIC_PARTITION_CONFIG = TASK_PREFIX.concat("leader.topic.partitions");


    // General Connector config
    // Topics - Note that ONE of source.topics or source.topics.regex can be set, not both.
    public static final String SOURCE_TOPICS_CONFIG =                   "source.topics";
    public static final String SRC_TOPICS_DOC =                         "List of topics to consume";
    public static final String SOURCE_TOPICS_DEFAULT =                  "";
    public static final String SOURCE_TOPICS_REGEX_CONFIG =             "source.topics.regex";
    public static final String SOURCE_TOPICS_REGEX_DOC =                "Regular expression giving topics to consume. " +
            "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>." +
            "Only one of " + SOURCE_TOPICS_CONFIG + " or " + SOURCE_TOPICS_REGEX_CONFIG + " should be specified.";
    public static final String SOURCE_TOPICS_REGEX_DEFAULT =            "";
    public static final String DESTINATION_TOPIC_PREFIX_CONFIG =        "destination.topics.prefix";
    public static final String DESTINATION_TOPIC_PREFIX_DOC =           "Prefix to add to topic names to generate the name of the Kafka topic to publish data to";
    public static final String DESTINATION_TOPIC_PREFIX_DEFAULT =       "";
    // Message headers
    public static final String INCLUDE_MESSAGE_HEADERS_CONFIG =         "include.message.headers";
    public static final String INCLUDE_MESSAGE_HEADERS_DOC =            "Indicates that message headers from source records should be included in output";
    public static final boolean INCLUDE_MESSAGE_HEADERS_DEFAULT =       true;
    // Other connector configs
    public static final String POLL_TIMEOUT_MS_CONFIG =         "poll.timeout.ms";
    public static final String POLL_TIMEOUT_MS_DOC =            "Maximum amount of time to wait in each poll loop without data before cancelling the poll and returning control to the worker task";
    public static final int POLL_TIMEOUT_MS_DEFAULT =           1000;
    public static final String MAX_SHUTDOWN_WAIT_MS_CONFIG =    "max.shutdown.wait.ms";
    public static final String MAX_SHUTDOWN_WAIT_MS_DOC =       "Maximum amount of time to wait before forcing the consumer to close";
    public static final int MAX_SHUTDOWN_WAIT_MS_DEFAULT =      1500;



    // General Source Kafka Config
    public static final String CONSUMER_BOOTSTRAP_SERVERS_CONFIG =          CONSUMER_PREFIX.concat("bootstrap.servers");
    public static final String CONSUMER_BOOTSTRAP_SERVERS_DOC =             "list of kafka brokers to use to bootstrap the source cluster";
    // These are the kafka consumer configs we override defaults for
    // Note that *any* kafka consumer config can be set by adding the
    // CONSUMER_PREFIX in front of the standard consumer config strings
    public static final String CONSUMER_MAX_POLL_RECORDS_CONFIG =           CONSUMER_PREFIX.concat("max.poll.records");
    public static final String CONSUMER_MAX_POLL_RECORDS_DOC =              "Maximum number of records to return from each poll of the consumer";
    public static final int CONSUMER_MAX_POLL_RECORDS_DEFAULT =             500;
    public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG =          CONSUMER_PREFIX.concat("auto.offset.reset");
    public static final String CONSUMER_AUTO_OFFSET_RESET_DOC =             "If there is no stored offset for a partition, where to reset from [earliest|latest].";
    public static final String CONSUMER_AUTO_OFFSET_RESET_DEFAULT =         "earliest";
    public static final String CONSUMER_KEY_DESERIALIZER_CONFIG =           CONSUMER_PREFIX.concat("key.deserializer");
    public static final String CONSUMER_KEY_DESERIALIZER_DOC =              "Key deserializer to use for the kafka consumers connecting to the source cluster.";
    public static final String CONSUMER_KEY_DESERIALIZER_DEFAULT =          "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public static final String CONSUMER_VALUE_DESERIALIZER_CONFIG =         CONSUMER_PREFIX.concat("value.deserializer");
    public static final String CONSUMER_VALUE_DESERIALIZER_DOC =            "Value deserializer to use for the kafka consumers connecting to the source cluster.";
    public static final String CONSUMER_VALUE_DESERIALIZER_DEFAULT =        "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    // Partition Monitor
    public static final String PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_CONFIG =                 PARTITION_MONITOR_PREFIX.concat("topic.list.timeout.ms");
    public static final String PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_DOC  =                   "Amount of time the partition monitor thread should wait for kafka to return topic information before logging a timeout error.";
    public static final int PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_DEFAULT =                   60000;
    public static final String PARTITION_MONITOR_POLL_INTERVAL_MS_CONFIG =                      PARTITION_MONITOR_PREFIX.concat("poll.interval.ms");
    public static final String PARTITION_MONITOR_POLL_INTERVAL_MS_DOC =                         "How long to wait before re-querying the source cluster for a change in the partitions to be consumed";
    public static final int PARTITION_MONITOR_POLL_INTERVAL_MS_DEFAULT =                        300000;
    public static final String PARTITION_MONITOR_RECONFIGURE_TASKS_ON_LEADER_CHANGE_CONFIG =    PARTITION_MONITOR_PREFIX.concat("reconfigure.tasks.on.leader.change");
    public static final String PARTITION_RECONFIGURE_TASKS_ON_LEADER_CHANGE_DOC =               "Indicates whether the partition monitor should request a task reconfiguration when partition leaders have changed";
    public static final boolean PARTITION_RECONFIGURE_TASKS_ON_LEADER_CHANGE_DEFAULT =          false;




    // Config definition
    public static ConfigDef CONFIG = new ConfigDef()
        .define(SOURCE_TOPICS_CONFIG, Type.LIST, SOURCE_TOPICS_DEFAULT, ConfigDef.Importance.HIGH, SRC_TOPICS_DOC)
        .define(SOURCE_TOPICS_REGEX_CONFIG, Type.STRING, SOURCE_TOPICS_REGEX_DEFAULT, ConfigDef.Importance.HIGH, SOURCE_TOPICS_REGEX_DOC)
        .define(DESTINATION_TOPIC_PREFIX_CONFIG, Type.STRING, DESTINATION_TOPIC_PREFIX_DEFAULT, Importance.MEDIUM, DESTINATION_TOPIC_PREFIX_DOC)
        .define(INCLUDE_MESSAGE_HEADERS_CONFIG, Type.BOOLEAN, INCLUDE_MESSAGE_HEADERS_DEFAULT, Importance.MEDIUM, INCLUDE_MESSAGE_HEADERS_DOC)
        .define(PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_CONFIG, Type.INT, PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_DEFAULT, Importance.LOW, PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_DOC)
        .define(PARTITION_MONITOR_POLL_INTERVAL_MS_CONFIG, Type.INT, PARTITION_MONITOR_POLL_INTERVAL_MS_DEFAULT, Importance.MEDIUM, PARTITION_MONITOR_POLL_INTERVAL_MS_DOC)
        .define(PARTITION_MONITOR_RECONFIGURE_TASKS_ON_LEADER_CHANGE_CONFIG, Type.BOOLEAN, PARTITION_RECONFIGURE_TASKS_ON_LEADER_CHANGE_DEFAULT, Importance.MEDIUM, PARTITION_RECONFIGURE_TASKS_ON_LEADER_CHANGE_DOC)
        .define(POLL_TIMEOUT_MS_CONFIG, Type.INT, POLL_TIMEOUT_MS_DEFAULT, Importance.LOW, POLL_TIMEOUT_MS_DOC)
        .define(MAX_SHUTDOWN_WAIT_MS_CONFIG, Type.INT, MAX_SHUTDOWN_WAIT_MS_DEFAULT, Importance.LOW, MAX_SHUTDOWN_WAIT_MS_DOC)
        .define(CONSUMER_BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Importance.HIGH, CONSUMER_BOOTSTRAP_SERVERS_DOC)
        .define(CONSUMER_MAX_POLL_RECORDS_CONFIG, Type.INT, CONSUMER_MAX_POLL_RECORDS_DEFAULT, Importance.LOW, CONSUMER_MAX_POLL_RECORDS_DOC)
        .define(CONSUMER_AUTO_OFFSET_RESET_CONFIG, Type.STRING, CONSUMER_AUTO_OFFSET_RESET_DEFAULT, Importance.MEDIUM, CONSUMER_AUTO_OFFSET_RESET_DOC)
        .define(CONSUMER_KEY_DESERIALIZER_CONFIG, Type.STRING, CONSUMER_KEY_DESERIALIZER_DEFAULT, Importance.LOW, CONSUMER_KEY_DESERIALIZER_DOC)
        .define(CONSUMER_VALUE_DESERIALIZER_CONFIG, Type.STRING, CONSUMER_VALUE_DESERIALIZER_DEFAULT, Importance.LOW, CONSUMER_VALUE_DESERIALIZER_DOC);

    public KafkaSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }


    // Returns all values with a specified prefix with the prefix stripped from the key
    public Map<String, Object> allWithPrefix(String prefix) {
        return allWithPrefix(prefix, true);
    }

    // Returns all values with a specified prefix with the prefix stripped from the key if desired
    // Original input is set first, then overwritten (if applicable) with the parsed values
    public Map<String, Object> allWithPrefix(String prefix, boolean stripPrefix) {
        Map<String, Object> result = originalsWithPrefix(prefix, stripPrefix);
        for (Map.Entry<String, ?> entry : values().entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                if (stripPrefix)
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                else
                    result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    // Returns all values (part of definition or original strings) as strings so they can be used with functions accepting Map<String,String> configs
    public Map<String, String> allAsStrings() {
        Map<String, String> result = new HashMap<>();
        result.put(DESTINATION_TOPIC_PREFIX_CONFIG, getString(DESTINATION_TOPIC_PREFIX_CONFIG));
        result.put(POLL_TIMEOUT_MS_CONFIG, String.valueOf(getInt(POLL_TIMEOUT_MS_CONFIG)));
        result.put(MAX_SHUTDOWN_WAIT_MS_CONFIG, String.valueOf(getInt(MAX_SHUTDOWN_WAIT_MS_CONFIG)));
        result.put(CONSUMER_MAX_POLL_RECORDS_CONFIG, String.valueOf(getInt(CONSUMER_MAX_POLL_RECORDS_CONFIG)));
        result.put(CONSUMER_AUTO_OFFSET_RESET_CONFIG, getString(CONSUMER_AUTO_OFFSET_RESET_CONFIG));
        result.put(CONSUMER_KEY_DESERIALIZER_CONFIG, getString(CONSUMER_KEY_DESERIALIZER_CONFIG));
        result.put(CONSUMER_VALUE_DESERIALIZER_CONFIG, getString(CONSUMER_VALUE_DESERIALIZER_CONFIG));
        result.put(PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_CONFIG, String.valueOf(getInt(PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_CONFIG)));
        result.put(PARTITION_MONITOR_POLL_INTERVAL_MS_CONFIG, String.valueOf(getInt(PARTITION_MONITOR_POLL_INTERVAL_MS_CONFIG)));
        result.put(PARTITION_MONITOR_RECONFIGURE_TASKS_ON_LEADER_CHANGE_CONFIG, String.valueOf(getBoolean(PARTITION_MONITOR_RECONFIGURE_TASKS_ON_LEADER_CHANGE_CONFIG)));
        result.putAll(originalsStrings()); // Will set any values without defaults and will capture additional configs like consumer settings
        return result;
    }

}