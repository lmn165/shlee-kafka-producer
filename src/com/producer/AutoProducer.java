package com.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AutoProducer {
	private static final String TOPIC = "exam";
	private static final String SERVERS = "localhost:9092";

	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put("bootstrap.servers", SERVERS);
		prop.put("key.serializer", StringSerializer.class.getName());
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		int cnt = 1;
		while(true) {
			
			ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "message " + cnt++);
			try {
				producer.send(record, (metadata, exception) -> {
					if (exception != null) {
						System.out.println("some exception occur!");
					}
				});
				Thread.sleep(3000);
			} catch (Exception e) {
				System.out.println("exception: " + e);
			} finally {
				producer.flush();
			}

			if (cnt > 10) {
				producer.send(new ProducerRecord<String, String>(TOPIC, "It's Done!!"));
				producer.close();
				break;
			}
		}
	}

}
