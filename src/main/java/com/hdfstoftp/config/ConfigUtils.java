package com.hdfstoftp.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.pool.impl.contrib.FTPClientConfigure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 配置管理工具类
 * @author wanghouda
 *
 */
/**
 * @author heaven
 *
 */
/**
 * @author heaven
 *
 */
public class ConfigUtils {
	
	private static final Logger logger = LoggerFactory.getLogger("file");
	
	/**
	 * ftpClient的配置
	 */
	public static final FTPClientConfigure ftpClientConfig;
	/**
	 * rename的目录
	 */
	public static final String RENAME_DIR="uploaded";
	
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
	 * 静态方法初始化加载配置
	 */
	static { 
		loadConfig();
		ftpClientConfig=getFTPClientConfig();
		logger.info(ftpClientConfig.toString());
	}
	
	
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
	public static String getType(String string,String defaultValue) {
		return props.getProperty(string,defaultValue);
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
		//return ConfigUtils.class.getClassLoader().getResource(fileName).getPath();
		return getConfigDir()+fileName;
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
		System.out.println(getType("ftp.host"));
		logger.info("teafdsfasdh");
	}

	/**
	 * 初始化ftpclient方法
	 * @return FTPClientConfig
	 */
	public static FTPClientConfigure getFTPClientConfig() {
		loadConfig();
		FTPClientConfigure config=new FTPClientConfigure();
		config.setHost(ConfigUtils.getType("ftp.host"));
		config.setPort(Integer.parseInt(ConfigUtils.getType("ftp.port")));
		config.setUsername(ConfigUtils.getType("ftp.username"));
		config.setPassword(ConfigUtils.getType("ftp.password"));
		config.setClientTimeout(Integer.parseInt(ConfigUtils.getType("ftp.timeout")));
		config.setPassiveMode(ConfigUtils.getType("ftp.passivemode", "false"));
		config.setEncoding(ConfigUtils.getType("ftp.encoding", "GBK"));
		config.setThreadNum(Integer.valueOf(ConfigUtils.getType("ftp.transfer.threadnum", "10")));
		config.setRenameUploaded("true".equals(ConfigUtils.getType("ftp.rename.uploaded"," true")));
		config.setRetryTimes(Integer.valueOf(ConfigUtils.getType("ftp.retry.times", "3")));
		config.setTransferFileType(Integer.valueOf(ConfigUtils.getType("ftp.retry.times", "2")));
		return config;
	}

}
