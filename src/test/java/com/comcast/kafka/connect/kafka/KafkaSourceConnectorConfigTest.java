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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import org.apache.kafka.common.config.ConfigException;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;

public class KafkaSourceConnectorConfigTest {

  // Minimum config
  private static final Map<String, String> defaultMap;
  static {
    Map<String, String> map = new HashMap<>();
    map.put(KafkaSourceConnectorConfig.SOURCE_TOPIC_WHITELIST_CONFIG, "test.topic");
    map.put(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG, "localhost:6000");
    map.put(KafkaSourceConnectorConfig.CONSUMER_GROUP_ID_CONFIG, "test-consumer-group");
    defaultMap = Collections.unmodifiableMap(map);
  }

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testMinimalConfigDoesNotThrow() {
    Map<String, String> configMap = new HashMap<>(defaultMap);
    new KafkaSourceConnectorConfig(configMap);
  }

  @Test(expected = ConfigException.class)
  public void testThrowsWithoutTopic() {
    Map<String, String> configMap = new HashMap<>(defaultMap);
    configMap.remove(KafkaSourceConnectorConfig.SOURCE_TOPIC_WHITELIST_CONFIG);
    new KafkaSourceConnectorConfig(configMap);
  }

  @Test(expected = ConfigException.class)
  public void testThrowsWithoutBootstrapServers() {
    Map<String, String> configMap = new HashMap<>(defaultMap);
    configMap.remove(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG);
    new KafkaSourceConnectorConfig(configMap);
  }

  @Test(expected = ConfigException.class)
  public void testThrowsWithoutConsumerGroup() {
    Map<String, String> configMap = new HashMap<>(defaultMap);
    configMap.remove(KafkaSourceConnectorConfig.CONSUMER_GROUP_ID_CONFIG);
    new KafkaSourceConnectorConfig(configMap);
  }

  @Test()
  public void testCanSetSourceConsumerParametersWithSourcePrefix() {
    String configKey = "max.poll.records";
    Map<String, String> configMap = new HashMap<>(defaultMap);
    String sourceMaxPollRecordsString = KafkaSourceConnectorConfig.SOURCE_PREFIX.concat(configKey);
    String maxPollRecords = "123456";
    configMap.put(sourceMaxPollRecordsString, maxPollRecords);
    Properties consumerProps = new KafkaSourceConnectorConfig(configMap).getKafkaConsumerProperties();
    assertEquals(maxPollRecords, String.valueOf(consumerProps.get(configKey)));
  }

  @Test()
  public void testCanSetSourceConsumerParametersWithConsumerPrefix() {
    String configKey = "max.poll.records";
    Map<String, String> configMap = new HashMap<>(defaultMap);
    String consumerMaxPollRecordsString = KafkaSourceConnectorConfig.CONSUMER_PREFIX.concat(configKey);;
    String maxPollRecords = "123456";
    configMap.put(consumerMaxPollRecordsString, maxPollRecords);
    Properties consumerProps = new KafkaSourceConnectorConfig(configMap).getKafkaConsumerProperties();
    assertEquals(maxPollRecords, String.valueOf(consumerProps.get(configKey)));
  }

  @Test()
  public void testConsumerPrefixOverridesSourcePrefix() {
    String configKey = "max.poll.records";
    Map<String, String> configMap = new HashMap<>(defaultMap);
    String sourceMaxPollRecordsString = KafkaSourceConnectorConfig.SOURCE_PREFIX.concat(configKey);
    String consumerMaxPollRecordsString = KafkaSourceConnectorConfig.CONSUMER_PREFIX.concat(configKey);
    String maxPollRecords = "123456";
    configMap.put(sourceMaxPollRecordsString, "0");
    configMap.put(consumerMaxPollRecordsString, maxPollRecords);
    Properties consumerProps = new KafkaSourceConnectorConfig(configMap).getKafkaConsumerProperties();
    assertEquals(maxPollRecords, String.valueOf(consumerProps.get(configKey)));
  }





}