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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


/**
 * KafkaConnector is a Kafka Connect Connector implementation that generates tasks
 * to ingest messages from a source kafka cluster
 */

public class KafkaSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceConnector.class);
    private KafkaSourceConnectorConfig connectorConfig;

    private PartitionMonitor partitionMonitor;

    @Override
    public String version() {
        return Version.version();
    }

    @Override
    public void start(Map<String, String> config) throws ConfigException {
        LOG.info("Connector: start()");
        connectorConfig = new KafkaSourceConnectorConfig(config);
        if (connectorConfig.getList(KafkaSourceConnectorConfig.CONSUMER_BOOTSTRAP_SERVERS_CONFIG).isEmpty()) {
            throw new ConfigException("At least one bootstrap server must be configured in " + KafkaSourceConnectorConfig.CONSUMER_BOOTSTRAP_SERVERS_CONFIG);
        }
        // Topic config needs to be validated first
        List<String> topicsList = connectorConfig.getList(KafkaSourceConnectorConfig.SOURCE_TOPICS_CONFIG);
        boolean topicsListPresent = !topicsList.isEmpty();
        String topicsRegexStr = connectorConfig.getString(KafkaSourceConnectorConfig.SOURCE_TOPICS_REGEX_CONFIG);
        boolean topicsRegexStrPresent = !topicsRegexStr.trim().isEmpty();
        if (topicsListPresent && topicsRegexStrPresent) {
            throw new ConfigException(KafkaSourceConnectorConfig.SOURCE_TOPICS_CONFIG + " and " + KafkaSourceConnectorConfig.SOURCE_TOPICS_REGEX_CONFIG +
                    " are mutually exclusive options, but both are set.");
        }
        if (!topicsListPresent && !topicsRegexStrPresent) {
            throw new ConfigException("Must configure one of " + KafkaSourceConnectorConfig.SOURCE_TOPICS_CONFIG + " or " + KafkaSourceConnectorConfig.SOURCE_TOPICS_REGEX_CONFIG);
        }
        // Other config
        int shutdownTimeout = connectorConfig.getInt(KafkaSourceConnectorConfig.MAX_SHUTDOWN_WAIT_MS_CONFIG);
        int topicRequestTimeoutMs = connectorConfig.getInt(KafkaSourceConnectorConfig.PARTITION_MONITOR_TOPIC_LIST_TIMEOUT_MS_CONFIG);
        boolean reconfigureTasksOnLeaderChange = connectorConfig.getBoolean(KafkaSourceConnectorConfig.PARTITION_MONITOR_RECONFIGURE_TASKS_ON_LEADER_CHANGE_CONFIG);
        int pollIntervalMs = connectorConfig.getInt(KafkaSourceConnectorConfig.PARTITION_MONITOR_POLL_INTERVAL_MS_CONFIG);
        Properties adminClientConfig = new Properties();
        adminClientConfig.putAll(connectorConfig.allWithPrefix(KafkaSourceConnectorConfig.CONSUMER_PREFIX));
        LOG.info("Starting Partition Monitor to monitor source kafka cluster partitions");
        if (topicsListPresent) {
            partitionMonitor = new PartitionMonitor(context, adminClientConfig, topicsList, reconfigureTasksOnLeaderChange, pollIntervalMs, topicRequestTimeoutMs, shutdownTimeout);
        } else {
            partitionMonitor = new PartitionMonitor(context, adminClientConfig, topicsRegexStr, reconfigureTasksOnLeaderChange, pollIntervalMs, topicRequestTimeoutMs, shutdownTimeout);
        }
        partitionMonitor.start();
    }


    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSourceTask.class;
    }


    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> leaderTopicPartitions = partitionMonitor.getCurrentLeaderTopicPartitions()
            .stream()
            .map(LeaderTopicPartition::toString)
            .sorted() // Minor task performance/overhead improvement by roughly grouping tasks and leaders
            .collect(Collectors.toList());
        int taskCount = Math.min(maxTasks, leaderTopicPartitions.size());
        if (taskCount < 1) {
            LOG.info("No tasks to start.");
            return new ArrayList<>();
        }
        return ConnectorUtils.groupPartitions(leaderTopicPartitions, taskCount)
            .stream()
            .map(leaderTopicPartitionsGroup -> {
                Map<String, String> taskConfig = new HashMap<>();
                taskConfig.putAll(connectorConfig.allAsStrings());
                taskConfig.put(KafkaSourceConnectorConfig.TASK_LEADER_TOPIC_PARTITION_CONFIG, String.join(",", leaderTopicPartitionsGroup));
                return taskConfig;
            })
            .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        LOG.info("Connector received stop(). Cleaning Up.");
        partitionMonitor.shutdown();
        LOG.info("Stopped.");
    }

    @Override
    public ConfigDef config () {
        LOG.info("Connector: config()");
        return KafkaSourceConnectorConfig.CONFIG;
    }
    
}
