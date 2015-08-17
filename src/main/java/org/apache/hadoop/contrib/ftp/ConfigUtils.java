package org.apache.hadoop.contrib.ftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * 配置管理工具类
 * @author wanghouda
 *
 */
/**
 * @author heaven
 *
 */
public class ConfigUtils {
	
	
	/**
	 * 日期格式常量-精确到日
	 */
	public static final String FORMAT_DATE="yyyyMMdd";
	/**
	 * 日期格式常量-精确到小时
	 */
	public static final String FORMAT_HOUR="yyyyMMddHH";
	/**
	 * 日期格式常量-精确到秒
	 */
	public static final String FORMAT_SS="yyyyMMddHHmmss";
	/**
	 * 声明一个配置文件对象
	 */
	private static Properties props = new Properties();
	
	/**
	 * 声明配置文件名称
	 */
	public static final String CONF_FILE_NAME="hdfs-on-ftp.properties";
	
	/**
	 * 加载配置文件方法
	 */
	public static void loadConfig()  {
		try {
			props.load(new FileInputStream(loadResource(CONF_FILE_NAME)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
			
		}
	}
	
	/**
	 * 获取配置文件的file对象方法
	 * @param resourceName
	 * @return File
	 */
	private static File loadResource(String resourceName) {
		return new File(getFilePath(resourceName));
	}
	
	/**
	 * 获取配置文件的内容值
	 * @param string
	 * @return String
	 */
	public static String getType(String string) {
		return props.getProperty(string);
	}
	
	/**
	 * 获取配置路径
	 * @param fileName
	 * @return String
	 */
	public static String getFilePath(String fileName) {
		return ConfigUtils.class.getClassLoader().getResource(fileName).getPath();
	}
	
	/**
	 * 获取配置目录
	 * @return String
	 */
	public static String getConfigDir() {
		String systemPath=System.getProperty("user.dir") ;;
		return new StringBuffer().append(systemPath).append(File.separator).append("conf").append(File.separator).toString();
	}
	
	/**
	 * 测试用的main方法
	 * @param args
	 */
	public static void main(String[] args) {
		loadConfig();
	}

}
