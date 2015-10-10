package com.hdfstoftp.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.pool.impl.contrib.FTPClientConfigure;

/**
 * 配置类
 * @author heaven
 *
 */
public class Config {
	
	/**
	 * 默认配置文件名称
	 */
	private static final String DEFAULT_CONFIG_PATH="conf"+File.separator+"hdfs-to-ftp.properties";

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
	 * 加载配置文件
	 */
	private Properties properties=new Properties();
	/**
	 * 参数信息
	 */
	public CommandLine commandLine;
	/**
	 * FTPClient相关配置
	 */
	public  FTPClientConfigure ftpClientConfig;
	
	
	
	public Config(String[] args) throws ParseException, FileNotFoundException, IOException{
		//处理参数方法
		handCommandLine(args);
		//加载FTPClient配置方法
	    ftpClientConfig=getFTPClientConfig();
	}
	
	private void handCommandLine(String[] args) throws ParseException, FileNotFoundException, IOException {
		//设置命令行选项
		Options options = new Options();
		Option option = new Option("t", "time", true, "files modified before this time will be uploaded,the time formate should be yyyyMMddHHmmss");//配置文件必须指定
	    option.setRequired(false);
	    options.addOption(option);
	    option = new Option("r", "regex", true, "only upload the files matched the specified java regex");
	    option.setRequired(false);
	    options.addOption(option);
	    option = new Option("d", "day", true, "only upload the files modified in a specified day");
	    option.setRequired(false);
	    options.addOption(option);
	    option = new Option("h", "hour", true, "only upload the files modified in a specified hour");
	    option.setRequired(false);
	    options.addOption(option);
	    option = new Option("c", "ftpClientConfig path", true, "the path of ftpClientConfig file");
	    option.setRequired(false);
	    options.addOption(option);
	    option = new Option("o", "overwrite", true, "overwrite flag, default value is false");
	    option.setRequired(false);
	    options.addOption(option);
	    
	    DefaultParser parser=new DefaultParser();
	    commandLine=parser.parse(options, args);
	    if(!commandLine.hasOption("c")){
	    	String configFilePath=System.getProperty("user.dir")+File.separator+DEFAULT_CONFIG_PATH ;
	    	properties.load(new FileInputStream(configFilePath));
	    }else{
	    	properties.load(new FileInputStream(commandLine.getOptionValue("c").trim()));
	    }
	    if(commandLine.getArgs().length<2){
	    	throw new MissingArgumentException("please input both the source and dest dir!");
	    }
	    souceDir=commandLine.getArgs()[0];
	    destDir=commandLine.getArgs()[1];
	    threadNum=Integer.valueOf(properties.getProperty("app.transfer.threadnum","3"));
	    renameUploaded="true".equals(properties.getProperty("app.uploaded.isrename","true"));
	    retryTimes=Integer.valueOf(properties.getProperty("app.retry.times","3"));
	}
	private String souceDir;
	private String destDir;
	private int threadNum;
	private boolean renameUploaded;
	private int retryTimes;
	

	public  FTPClientConfigure getFTPClientConfig() {
		ftpClientConfig=new FTPClientConfigure();
		ftpClientConfig.setHost(properties.getProperty("ftp.host"));
		ftpClientConfig.setPort(Integer.parseInt(properties.getProperty("ftp.port")));
		ftpClientConfig.setUsername(properties.getProperty("ftp.username"));
		ftpClientConfig.setPassword(properties.getProperty("ftp.password"));
		ftpClientConfig.setClientTimeout(Integer.parseInt(properties.getProperty("ftp.timeout")));
		ftpClientConfig.setPassiveMode(properties.getProperty("ftp.passivemode", "false"));
		ftpClientConfig.setEncoding(properties.getProperty("ftp.encoding", "GBK"));
		ftpClientConfig.setTransferFileType(Integer.valueOf(properties.getProperty("ftp.retry.times", "2")));
		return ftpClientConfig;
	}
	
	public static void main(String[] args) throws FileNotFoundException, ParseException, IOException {
		
		args=new String[] {"d:/input","e:/output","-c D:\\input\\hdfs-to-ftp.properties"};
		System.out.println(Arrays.toString(args));
		Config config=new Config(args);
		System.out.println(config);
	}
	
	@Override
	public String toString() {
		return "Config ["
				+ "\n souceDir=" + souceDir 
				+ "\n destDir=" + destDir 
				+ "\n " + ftpClientConfig 
				+ "\n threadNum=" + threadNum 
				+ "\n renameUploaded=" + renameUploaded
				+ "\n retryTimes=" + retryTimes 
				+ "\n]";
	}

	public String getSouceDir() {
		return souceDir;
	}

	public void setSouceDir(String souceDir) {
		this.souceDir = souceDir;
	}

	public String getDestDir() {
		return destDir;
	}

	public void setDestDir(String destDir) {
		this.destDir = destDir;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	public boolean isRenameUploaded() {
		return renameUploaded;
	}

	public void setRenameUploaded(boolean renameUploaded) {
		this.renameUploaded = renameUploaded;
	}

	public int getRetryTimes() {
		return retryTimes;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}

	public CommandLine getCommandLine() {
		return commandLine;
	}

	public void setCommandLine(CommandLine commandLine) {
		this.commandLine = commandLine;
	}
	
	
}
