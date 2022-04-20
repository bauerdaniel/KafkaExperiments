package com.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sun.misc.Signal;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class Consumer2 {

    public static volatile boolean shutdown = false;

    public static void main(String[] args) throws IOException {

        Signal.handle(new Signal("INT"), signal -> shutdown = true);

        ObjectMapper mapper = new ObjectMapper();
        Histogram global = new Histogram(1, 10000000, 2);

        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<>(properties);

            // Subscribe to two topics -> a list is created
            consumer.subscribe(Arrays.asList("user-stories", "messages"));
            int timeouts = 0;

            while (!shutdown) {
                ConsumerRecords<String, String> records = consumer.poll(200);
                if (records.count() == 0) {
                    timeouts++;
                } else {
                    System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                    timeouts = 0;
                }
                for (ConsumerRecord<String, String> record : records) {
                    switch (record.topic()) {
                        case "user-stories":
                            JsonNode msg = mapper.readTree(record.value());
                            switch (msg.get("type").asText()) {
                                case "user":
                                    long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
                                    global.recordValue(latency);
                                    break;
                                case "stories":
                                    System.out.printf("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                            global.getTotalCount(),
                                            global.getValueAtPercentile(0), global.getValueAtPercentile(100),
                                            global.getMean(), global.getValueAtPercentile(99));
                                    break;
                                default:
                                    throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
                            }
                            break;
                        case "messages":
                            break;
                        default:
                            throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                    }
                }
            }

            System.out.println("Saving histogram");
            global.outputPercentileDistribution(new PrintStream("globalogram.hgrm"), 1.0);
        }
    }
}
