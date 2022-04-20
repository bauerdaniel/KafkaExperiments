package com.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;


public class Producer {
    public static void main(String[] args) throws IOException {
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try {

            for (int i = 0; i < 100000; i++) {
                producer.send(new ProducerRecord<String, String>(
                        "user-stories",
                        String.format(Locale.US, "{\"type\":\"user\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));

                if (i % 1000 == 0) {
                    producer.send(new ProducerRecord<String, String>(
                            "user-stories",
                            String.format(Locale.US, "{\"type\":\"stories\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                    producer.send(new ProducerRecord<String, String>(
                            "messages",
                            String.format(Locale.US, "{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                    producer.flush();
                    System.out.println("Sent message number " + i);
                }
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }
}
