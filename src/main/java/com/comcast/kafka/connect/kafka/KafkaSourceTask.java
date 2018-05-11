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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class KafkaSourceTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceTask.class);
    private static final String TOPIC_PARTITION_KEY = "topic:partition";
    private static final String OFFSET_KEY = "offset";

    // Used to ensure we can be nice and call consumer.close() on shutdown
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    // Flag to the poll() loop that we are awaiting shutdown so it can clean up.
    private AtomicBoolean stop = new AtomicBoolean(false);
    // Flag to the stop() function that it needs to wait for poll() to wrap up before trying to close the kafka consumer.
    private AtomicBoolean poll = new AtomicBoolean(false);
    // Used to enforce synchronized access to stop and poll
    private final Object stopLock = new Object();

    // Settings
    private int maxShutdownWait;
    private int pollTimeout;
    private String topicPrefix;
    private boolean includeHeaders;

    // Consumer
    private KafkaConsumer<byte[], byte[]> consumer;

    // TODO: Maybe synchronize access to this function to ensure that stop() cant be called before the consumer is initialized? (Can stop even be called if start() hasnt returned?)
    public void start(Map<String, String> opts) {
        LOG.info("{}: starting", this);
        KafkaSourceConnectorConfig config = new KafkaSourceConnectorConfig(opts);
        maxShutdownWait = config.getInt(KafkaSourceConnectorConfig.MAX_SHUTDOWN_WAIT_MS_CONFIG);
        pollTimeout = config.getInt(KafkaSourceConnectorConfig.POLL_TIMEOUT_MS_CONFIG);
        topicPrefix = config.getString(KafkaSourceConnectorConfig.DESTINATION_TOPIC_PREFIX_CONFIG);
        includeHeaders = config.getBoolean(KafkaSourceConnectorConfig.INCLUDE_MESSAGE_HEADERS_CONFIG);
        String unknownOffsetResetPosition = config.getString(KafkaSourceConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        // Kafka consumer config
        Properties props = new Properties();
        props.putAll(config.allWithPrefix(KafkaSourceConnectorConfig.CONSUMER_PREFIX));
        // Get the leader topic partitions to work with
        List<LeaderTopicPartition> leaderTopicPartitions = Arrays.asList(opts.get(KafkaSourceConnectorConfig.TASK_LEADER_TOPIC_PARTITION_CONFIG)
            .split(","))
            .stream()
            .map(LeaderTopicPartition::fromString)
            .collect(Collectors.toList());
        // retrieve the existing offsets (if any) for the configured partitions
        List<Map<String, String>> offsetLookupPartitions = leaderTopicPartitions.stream()
                .map(leaderTopicPartition -> Collections.singletonMap(TOPIC_PARTITION_KEY, leaderTopicPartition.toTopicPartitionString()))
                .collect(Collectors.toList());
        Map<String, Long> topicPartitionStringsOffsets = context.offsetStorageReader().offsets(offsetLookupPartitions)
            .entrySet()
            .stream()
            .filter(e -> e != null && e.getKey() != null && e.getKey().get(TOPIC_PARTITION_KEY) != null && e.getValue() != null && e.getValue().get(OFFSET_KEY) != null)
            .collect(Collectors.toMap(e -> e.getKey().get(TOPIC_PARTITION_KEY), e -> (long) e.getValue().get(OFFSET_KEY)));
        // Set up Kafka consumer
        consumer = new KafkaConsumer<byte[], byte[]>(props);
        // Get topic partitions and offsets so we can seek() to them
        Map<TopicPartition, Long> topicPartitionOffsets = new HashMap<>();
        List<TopicPartition> topicPartitionsWithUnknownOffset = new ArrayList<>();
        for (LeaderTopicPartition leaderTopicPartition : leaderTopicPartitions) {
            String topicPartitionString = leaderTopicPartition.toTopicPartitionString();
            TopicPartition topicPartition = leaderTopicPartition.toTopicPartition();
            if (topicPartitionStringsOffsets.containsKey(topicPartitionString)) {
                topicPartitionOffsets.put(topicPartition, topicPartitionStringsOffsets.get(topicPartitionString));
            } else {
                // No stored offset? No worries, we will place it it the list to lookup
                topicPartitionsWithUnknownOffset.add(topicPartition);
            }
        }
        // Set default offsets for partitions without stored offsets
        if (topicPartitionsWithUnknownOffset.size() > 0) {
            Map<TopicPartition, Long> defaultOffsets;
            LOG.info("The following partitions do not have existing offset data: {}", topicPartitionsWithUnknownOffset);
            if (unknownOffsetResetPosition.equals("earliest")) {
                LOG.info("Using earliest offsets for partitions without existing offset data.");
                defaultOffsets = consumer.beginningOffsets(topicPartitionsWithUnknownOffset);
            } else if (unknownOffsetResetPosition.equals("latest")) {
                LOG.info("Using latest offsets for partitions without existing offset data.");
                defaultOffsets = consumer.endOffsets(topicPartitionsWithUnknownOffset);
            } else {
                LOG.warn("Config value {}, is set to an unknown value: {}. Partitions without existing offset data will not be consumed.", KafkaSourceConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG, unknownOffsetResetPosition);
                defaultOffsets = new HashMap<>();
            }
            topicPartitionOffsets.putAll(defaultOffsets);
        }
        // List of topic partitions to assign
        List<TopicPartition> topicPartitionsToAssign = new ArrayList<>(topicPartitionOffsets.keySet());
        consumer.assign(topicPartitionsToAssign);
        // Seek to desired offset for each partition
        topicPartitionOffsets.entrySet().forEach(e -> consumer.seek(e.getKey(), e.getValue()));
    }


    @Override
    public List<SourceRecord> poll() {
        if (LOG.isDebugEnabled()) LOG.debug("{}: poll()", this);
        synchronized (stopLock) {
            if (!stop.get())
                poll.set(true);
        }
        ArrayList<SourceRecord> records = new ArrayList<>();
        if (poll.get()) {
            try {
                ConsumerRecords<byte[], byte[]> krecords = consumer.poll(pollTimeout);
                if (LOG.isDebugEnabled()) LOG.debug("{}: Got {} records.", this, krecords.count());
                for (ConsumerRecord<byte[], byte[]> krecord : krecords) {
                    Map sourcePartition = Collections.singletonMap(TOPIC_PARTITION_KEY, krecord.topic().toString().concat(":").concat(Integer.toString(krecord.partition())));
                    Map sourceOffset = Collections.singletonMap(OFFSET_KEY, krecord.offset());
                    String destinationTopic = topicPrefix.concat(krecord.topic());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Task: sourceTopic:{} sourcePartition:{} sourceOffSet:{} destinationTopic:{}, key:{}, valueSize:{}",
                                krecord.topic(), krecord.partition(), krecord.offset(), destinationTopic, (krecord.key() != null) ? krecord.key().toString() : null, krecord.serializedValueSize()
                        );
                    }
                    if (includeHeaders){
                        // Mapping from source type: org.apache.kafka.common.header.Headers, to destination type: org.apache.kafka.connect.Headers
                        Headers sourceHeaders = krecord.headers();
                        ConnectHeaders destinationHeaders = new ConnectHeaders();
                        for (Header header: sourceHeaders) {
                            if (header != null) {
                                destinationHeaders.add(header.key(), header.value(), Schema.OPTIONAL_BYTES_SCHEMA);
                            }
                        }
                        records.add(new SourceRecord(sourcePartition, sourceOffset, destinationTopic, null, Schema.OPTIONAL_BYTES_SCHEMA, krecord.key(), Schema.OPTIONAL_BYTES_SCHEMA, krecord.value(), krecord.timestamp(), destinationHeaders));
                    } else {
                        records.add(new SourceRecord(sourcePartition, sourceOffset, destinationTopic, null, Schema.OPTIONAL_BYTES_SCHEMA, krecord.key(), Schema.OPTIONAL_BYTES_SCHEMA, krecord.value(), krecord.timestamp()));

                    }
                }
            } catch (WakeupException e) {
                LOG.info("{}: Caught WakeupException. Probably shutting down.", this);
            }
        }
        synchronized (stopLock) {
            // Existing poll(), set concurrency flag
            poll.set(false);
            // If stop has been set  processing, then stop the consumer.
            if (LOG.isDebugEnabled()) LOG.trace("{}: stop.get() = {}", this, stop.get());
            if (stop.get()) {
                LOG.info("{}: stop flag set during poll(), opening stopLatch", this);
                stopLatch.countDown();
                // If stopping, return immediately without records.
                return null;
            }
        }
        if (LOG.isDebugEnabled()) LOG.debug("{}: Returning {} records to connect", this, records.size());
        return records;
    }

    @Override
    public synchronized void stop() {
        long startWait = System.currentTimeMillis();
        synchronized (stopLock) {
            stop.set(true);
            LOG.info("{}: stop() called. Waking up consumer and shutting down", this);
            consumer.wakeup();
            if (LOG.isDebugEnabled()) LOG.trace("{}: poll.get() = {}", this, poll.get());
            if(!poll.get()) {
                LOG.info("{}: poll() not active, shutting down consumer.", this);
                consumer.close(Math.min(0, maxShutdownWait - (System.currentTimeMillis() - startWait)), TimeUnit.MILLISECONDS);
            } else {
                LOG.info("{}: poll() active, awaiting stopLatch before shutting down consumer", this);
                try {
                    stopLatch.await(Math.min(0, maxShutdownWait - (System.currentTimeMillis() - startWait)), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e){
                    LOG.warn("{}: Got InterruptedException while waiting on stopLatch", this);
                } finally {
                    LOG.info("{}: Shutting down consumer", this);
                    consumer.close(Math.min(0, maxShutdownWait - (System.currentTimeMillis() - startWait)), TimeUnit.MILLISECONDS);
                }
            }
        }
        LOG.info("{}: stopped", this);
    }



    @Override
    public String version() {
        return new Version().version();
    }

    public String toString() {
        return "KafkaSourceTask@" + Integer.toHexString(hashCode());
    }


}