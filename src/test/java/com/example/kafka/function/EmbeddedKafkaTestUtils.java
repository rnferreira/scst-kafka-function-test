/*
 * Copyright (c) 2020-2021, MAN Truck & Bus AG and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 */

package com.example.kafka.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public final class EmbeddedKafkaTestUtils {

  private EmbeddedKafkaTestUtils() {
    // private constructor
  }

  public static Consumer<String, String> createConsumer(
      EmbeddedKafkaBroker broker, String... topicNames) {
    Map<String, Object> consumerProps = getConsumerProps(broker);
    DefaultKafkaConsumerFactory<String, String> cf =
        new DefaultKafkaConsumerFactory<>(consumerProps);
    Consumer<String, String> consumer = cf.createConsumer();
    consumer.subscribe(List.of(topicNames));
    return consumer;
  }

  public static KafkaProducer<String, String> createProducer(EmbeddedKafkaBroker broker) {
    Map<String, Object> producerProps = new HashMap<>(getProducerProps(broker));
    return new KafkaProducer<>(producerProps);
  }

  public static Map<String, Object> getProducerProps(EmbeddedKafkaBroker broker) {
    var producerProps = KafkaTestUtils.producerProps(broker);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return producerProps;
  }

  public static Map<String, Object> getConsumerProps(EmbeddedKafkaBroker broker) {
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps("group-" + UUID.randomUUID(), "false", broker);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return consumerProps;
  }
}
