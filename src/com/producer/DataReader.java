package com.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.ibatis.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class DataReader {
	private static final String TOPIC = "exam";
	private static final String SERVERS = "localhost:9092";

	public static void main(String[] args) {
		String resource = "resources/Link.properties";
		Properties properties = new Properties();
		try {
			Reader reader = Resources.getResourceAsReader(resource);
			properties.load(reader);

			//			properties에 설정된 path	
			String errPath = properties.getProperty("WatchDir") + File.separator 
					+ properties.getProperty("WatchFile");
//			System.out.println(errPath);
			
//			프로그램에서 실행되는 os에 맞게 변환된 path
			File path = new File(errPath);
//			System.out.println(path);
			
			BufferedReader bReader = new BufferedReader(new FileReader(path));
			
			String str;
			while ((str = bReader.readLine()) != null) {
				System.out.println(str);
			}
			
			Path oldfile = Paths.get(path.toString());
			Path newfile = Paths.get(new File(properties.getProperty("WatchDir") + File.separator 
					+File.separator+"backup"+ properties.getProperty("WatchFile")).toString());
			Files.move(oldfile, newfile);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		Properties prop = new Properties();
//		prop.put("bootstrap.servers", SERVERS);
//		prop.put("key.serializer", StringSerializer.class.getName());
//		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
//
//		int cnt = 1;
//		while(true) {
//			
//			ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "message " + cnt++);
//			try {
//				producer.send(record, (metadata, exception) -> {
//					if (exception != null) {
//						System.out.println("some exception occur!");
//					}
//				});
//				Thread.sleep(3000);
//			} catch (Exception e) {
//				System.out.println("exception: " + e);
//			} finally {
//				producer.flush();
//			}
//
//			if (cnt > 10) {
//				producer.send(new ProducerRecord<String, String>(TOPIC, "It's Done!!"));
//				producer.close();
//				break;
//			}
//		}
	}

}
