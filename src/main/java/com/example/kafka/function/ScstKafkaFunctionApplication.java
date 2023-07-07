package com.example.kafka.function;

import com.example.kafka.function.domain.MyEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.function.BiFunction;

@SpringBootApplication
public class ScstKafkaFunctionApplication {

  public static void main(String[] args) {
    SpringApplication.run(ScstKafkaFunctionApplication.class, args);
  }

  @Bean
  public BiFunction<MyEvent, Map<String, String>, Message<?>> process() {
    return (p, h) -> {
      var key = h.get(KafkaHeaders.RECEIVED_KEY);
      System.out.println("Received key - " + key);
      if (p == null) {
        System.out.println("Sending tombstone - " + key);
        return MessageBuilder.withPayload(KafkaNull.INSTANCE)
            .setHeaderIfAbsent(KafkaHeaders.KEY, key)
            .build();
      } else {
        var out = new MyEvent(p.aProperty(), p.aProperty() + "-test");
        System.out.println("Sending new event - key: " + key + ", payload: " + out);
        return MessageBuilder.withPayload(out).copyHeadersIfAbsent(h).build();
      }
    };
  }
}
