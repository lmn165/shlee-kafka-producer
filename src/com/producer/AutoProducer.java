package com.producer;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AutoProducer {
	private static final String TOPIC = "exam";
	private static final String SERVERS = "localhost:9092";
	private static final String FIN_MESSAGE = "exit";

	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put("bootstrap.servers", SERVERS);
		prop.put("key.serializer", StringSerializer.class.getName());
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		while(true) {
			Scanner sc = new Scanner(System.in);
			System.out.print("Producing > ");
			String message = sc.nextLine();

			ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
			try {
				producer.send(record, (metadata, exception) -> {
					if (exception != null) {
						System.out.println("some exception occur!");
					}
				});
			} catch (Exception e) {
				System.out.println("exception: " + e);
			} finally {
				producer.flush();
			}

			if (FIN_MESSAGE.equals(message)) {
				producer.close();
				break;
			}
		}
	}

}
