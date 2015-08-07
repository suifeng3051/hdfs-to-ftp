package org.apache.hadoop.contrib.ftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtils {
	
	public static final String FORMAT_DATE="yyyyMMdd";
	public static final String FORMAT_HOUR="yyyyMMddHH";
	public static final String FORMAT_SS="yyyyMMddHHmmss";
	private static Properties props = new Properties();
	public static void loadConfig()  {
		try {
			props.load(new FileInputStream(loadResource("hdfs-on-ftp.properties")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
			
		}
	}
	private static File loadResource(String resourceName) {
		
		return new File(getFilePath(resourceName));
	}
	public static String getType(String string) {
		
		return props.getProperty(string);
	}
	public static String getFilePath(String fileName) {
		return ConfigUtils.class.getClassLoader().getResource(fileName).getPath();
	}
	public static String getConfigDir() {
		String systemPath=System.getProperty("user.dir") ;;
		return new StringBuffer().append(systemPath).append(File.separator).append("conf").append(File.separator).toString();
	}
	public static void main(String[] args) {
		loadConfig();
		System.out.println(ConfigUtils.getType("ftp.username"));
	}

}
