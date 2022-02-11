package com.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;

public class ReadingProducer {
	private static final String TOPIC = "exam";
	private static String[] features = {"date","total","stt","ta","gap"};

	public static void main(String[] args) {
		String resource = "resources/Link.properties";
		Properties properties = new Properties();
		KafkaProducer<String, String> producer = null;
		
		try {
			Reader reader = Resources.getResourceAsReader(resource);
			properties.load(reader);
			properties.put("key.serializer", StringSerializer.class.getName());
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			String fileName = (args.length == 0 ? properties.getProperty("WatchFile") : args[0]+".dat");
			String errPath = properties.getProperty("WatchDir") + File.separator + fileName;
			File path = new File(errPath);
//			System.out.println(Resources.getResourceURL(resource));

			BufferedReader bReader = new BufferedReader(new FileReader(path));
			
			producer = new KafkaProducer<String, String>(properties);
			
			String str;
			while ((str = bReader.readLine()) != null) {
				Map<String, String> data = new HashMap<>();
				int idx = 0;
				for (String val : str.split("\\|")) {
					data.put(features[idx++], val);
				}
				JSONObject jsonob = new JSONObject(data);
				
				ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, jsonob.toString());
				producer.send(record, (metadata, exception) -> {
					if (exception != null) {
						System.out.println("some exception occur!");
					}
				});
				Thread.sleep(Integer.parseInt(properties.getProperty("WatchDT")));
			}
			
			bReader.close();
			Path oldfile = Paths.get(path.toString());
			Path newfile = Paths.get(new File(properties.getProperty("WatchDir") + File.separator 
					+ "backup" + File.separator + fileName).toString());
			Files.move(oldfile, newfile);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
//			producer.send(new ProducerRecord<String, String>(TOPIC, properties.getProperty("ExitMessage")));
			producer.flush();
			producer.close();
		}
	}

}
