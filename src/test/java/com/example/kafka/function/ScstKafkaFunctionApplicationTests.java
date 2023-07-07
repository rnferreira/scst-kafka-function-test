package com.example.kafka.function;

import com.example.kafka.function.domain.MyEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = {
      "listeners=PLAINTEXT://localhost:9092",
      "port=9092",
      "delete.topic.enable=true",
      "auto.create.topics.enable=true"
    })
class ScstKafkaFunctionApplicationTests {

  @Autowired private EmbeddedKafkaBroker broker;

  @Test
  void contextLoads() {}

  @Test
  void testKafkaNull() throws ExecutionException, InterruptedException {
    // when
    try (var producer = EmbeddedKafkaTestUtils.createProducer(this.broker)) {
      producer.send(new ProducerRecord<>("in", "my-key-1", null)).get();
    }

    try (var consumer = EmbeddedKafkaTestUtils.createConsumer(this.broker, "out")) {

      await()
          .untilAsserted(
              () -> {
                var records = consumer.poll(Duration.ofMillis(100L));
                records.forEach(System.out::println);
                assertThat(records)
                    .hasSize(1)
                    .allSatisfy(
                        record -> {
                          assertThat(record)
                              .extracting(ConsumerRecord::key, ConsumerRecord::value)
                              .containsExactly("my-key-1", null);
                        });
              });
    }
  }
}
