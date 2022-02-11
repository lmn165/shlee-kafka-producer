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

public class DataReader {

	public static void main(String[] args) {
		String resource = "resources/Link.properties";
		Properties properties = new Properties();
		try {
			Reader reader = Resources.getResourceAsReader(resource);
			properties.load(reader);

			// properties에 설정된 path	
			String errPath = properties.getProperty("WatchDir") + File.separator 
					+ properties.getProperty("WatchFile");
//			System.out.println(errPath);
			
			// 프로그램에서 실행되는 os에 맞게 변환된 path
			File path = new File(errPath);
//			System.out.println(path);
			
			BufferedReader bReader = new BufferedReader(new FileReader(path));
			
			String str;
			while ((str = bReader.readLine()) != null) {
				System.out.println(str);
			}
			bReader.close();
			
			Path oldfile = Paths.get(path.toString());
			Path newfile = Paths.get(new File(properties.getProperty("WatchDir") + File.separator 
					+"backup"+File.separator+ properties.getProperty("WatchFile")).toString());
			Files.move(oldfile, newfile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
