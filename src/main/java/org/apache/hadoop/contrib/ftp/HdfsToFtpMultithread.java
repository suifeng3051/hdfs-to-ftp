package org.apache.hadoop.contrib.ftp;

/**
 * Licensed to the ctyun,this can be only used in ctyun company
 *
 */

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 直接拷贝hdfs中的文件到ftp文件系统方法
 * 
 * @author 王厚达
 * @mail wanghouda@126.com
 */
public class HdfsToFtpMultithread {

	/**
	 * 获取日志记录对象
	 */
	private static final Logger logger = LoggerFactory.getLogger(HdfsToFtpMultithread.class);
	
	/**
	 * 静态退出方法
	 * 
	 * @param str
	 */
	static void printAndExit(String str) {
		logger.error(str);
		System.exit(1);
	}

	/**
	 * 入口函数
	 * 
	 * @param argv
	 * @throws IOException
	 */
	
	public static void main(String[] argv) throws IOException {
		if (argv == null || argv.length < 2) {
			printAndExit("输入参数不合法!");
			//argv=new String[]{"d:/input","/home/heaven/whd","[.*word.*]"};
		}
		String srcPath = argv[0];// 源路径参数
		String dstPath = argv[1];// 目标路径参数
		String queryStr = null;// 查询条件
		boolean overwrite = true;// 是否覆盖参数
		if (argv.length > 2) {
			if (argv[2].length() == 5 || argv[2].length() == 4) {
				overwrite = "true".equals(argv[2]);
			} else {
				queryStr = argv[2];
				if (argv.length > 3) {
					overwrite = "true".equals(argv[3]);
				}
			}
		}
		Log.info("You input param is:" + argv[0] + " " + argv[1] + " " + (argv.length > 2 ? argv[2] : "") + " " + (argv.length > 3 ? argv[3] : ""));
		Path hdfsPath = new Path(srcPath);
		Path ftpPath = new Path(dstPath);
		try {
			copyFromHDFSToFTP(hdfsPath, ftpPath, queryStr, overwrite);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			Log.info(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			Log.info(e.getMessage());
		}

	}

	/**
	 * 拷贝hdfs数据到ftp的主方法
	 * 
	 * @param hdfsPath
	 * @param ftpPath
	 * @param queryStr
	 * @param overwrite
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws ParseException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws FTPClientException 
	 */
	private static void copyFromHDFSToFTP(Path hdfsPath, Path ftpPath, String queryStr, boolean overwrite) throws IOException, URISyntaxException, ParseException, InterruptedException, ExecutionException, FTPClientException {
		// 获取hdfs文件系统
		Configuration conf = new Configuration();
		FileSystem hdfsSystem = FileSystem.get(conf);
		// 调用filetutils类的工具方法
		MultiThreadCopy.multiThreadCopy(hdfsSystem, hdfsPath, ftpPath, queryStr, false, overwrite);
	}
}