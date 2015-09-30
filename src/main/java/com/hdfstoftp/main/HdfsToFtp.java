package com.hdfstoftp.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Licensed to the ctyun,this can be only used in ctyun company
 *
 */

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool.impl.contrib.FTPClientPool;
import org.apache.commons.pool.impl.contrib.FtpClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hdfstoftp.config.ConfigUtils;
import com.hdfstoftp.service.UploadFileTask;
import com.hdfstoftp.util.FileUtils;
import com.hdfstoftp.util.FtpClientUtil;

/**
 * 直接拷贝hdfs中的文件到ftp文件系统方法
 * 
 * @author 王厚达
 * @mail wanghouda@126.com
 */
public class HdfsToFtp {

	/**
	 * 获取日志记录对象
	 */
	private static final Logger logger = LoggerFactory.getLogger("file");
	private static final Logger logger_failed = LoggerFactory.getLogger("failed");
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
			//printAndExit("输入参数不合法!");
			argv = new String[] { "D:/input", "/home/heaven/whd" };
			//argv = new String[] { "d:/failed", "/home/heaven/whd" };
			
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
	 * @throws Exception
	 */
	private static void copyFromHDFSToFTP(Path hdfsPath, Path ftpPath, String queryStr, boolean overwrite) throws Exception {
		// 获取hdfs文件系统
		Configuration conf = new Configuration();
		FileSystem hdfsSystem = FileSystem.get(conf);
		// 调用filetutils类的工具方法
		multiThreadCopy(hdfsSystem, hdfsPath, ftpPath, queryStr, false, overwrite);
	}

	/**
	 * 主拷贝方法
	 * 
	 * @param srcFS
	 *            文件系统
	 * @param src
	 *            源路径
	 * @param dst
	 *            目标路径
	 * @param queryStr
	 *            过滤字符串
	 * @param deleteSource
	 *            是否删除源文件
	 * @param overwrite
	 *            是否覆盖相同文件名文件
	 * @return boolean
	 * @throws Exception
	 */
	private static boolean multiThreadCopy(FileSystem srcFS, Path src, Path dst, String filterStr, boolean deleteSource, boolean overwrite) throws Exception {
		long start = System.currentTimeMillis();
		boolean isRename=ConfigUtils.ftpClientConfig.isRenameUploaded();
		int retryTimes=3;
		// 获取目标路径
		String dstPath = dst.toUri().getPath();
		FileStatus fileStatus = srcFS.getFileStatus(src);
		String subDir=null;
		if (fileStatus.isDirectory()) {// 若是一个目录
			if(isRename){//若需要把上传成功的rename
				//Date date =new Date();
				//SimpleDateFormat sdf=new SimpleDateFormat(ConfigUtils.FORMAT_DATE);
				subDir=ConfigUtils.RENAME_DIR;
				srcFS.mkdirs(new Path(fileStatus.getPath(),subDir));
			}
			int threadNum=ConfigUtils.ftpClientConfig.getThreadNum();
			// 线程池
			ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
			// 获取ftp客户端
			FTPClientPool ftpPool = new FTPClientPool(threadNum, new FtpClientFactory(ConfigUtils.ftpClientConfig));
			FTPClient ftpClient = ftpPool.borrowObject();
			// 创建一个目标目录（如果乙存在默认不创建）
			ftpClient.makeDirectory(dstPath);
			ftpPool.returnObject(ftpClient);
			// 列出源目录中的文件信息
			FileStatus contents[] = srcFS.listStatus(src);
			long beginFilter = 0;
			long endFileter = 0;
			// 若有过滤条件
			if (filterStr != null) {
				if (filterStr.startsWith("[")) {// 若开头是"["，表示里面内容是正则表达式
					beginFilter = System.currentTimeMillis();
					contents = FileUtils.getFilterContents(filterStr, contents);
					endFileter = System.currentTimeMillis();
				} else {// 若不是以"["开头，则表示时间段
					beginFilter = System.currentTimeMillis();
					Long[] timeRange = FileUtils.parseTimeRange(filterStr);
					contents = FileUtils.getNewContents(timeRange, contents);
					endFileter = System.currentTimeMillis();
				}

			}
			logger.info("total file count:" + contents.length);
			Map<String, String> fileNameMap = null;
			long beginSkip = 0;
			long endSkip = 0;
			if (!overwrite) {// 若不覆盖，获取源路径已存在的文件名列表
				beginSkip = System.currentTimeMillis();
				fileNameMap = FtpClientUtil.getFileNameMap(dstPath);
				endSkip = System.currentTimeMillis();
			}
			int skiped = 0;

			List<Future<?>> futureList = new ArrayList<Future<?>>();
			for (int i = 0; i < contents.length; i++) {
				if (!overwrite && fileNameMap.containsKey(contents[i].getPath().getName())) {
					// 跳过已存在的文件
					skiped++;
					Log.info("skiped filename:" + contents[i].getPath().getName());
					continue;
				}
				if(contents[i].isDirectory()){
					continue;
				}
				// 用多线程的方式拷贝源文件到目标路径
				Future<?> future = threadPool.submit(new UploadFileTask(srcFS, contents[i].getPath(), new Path(dst, contents[i].getPath().getName()), ftpPool, deleteSource,isRename,subDir,retryTimes));
				futureList.add(future);
			}
			int transfered = 0;
			int failed = 0;
			for (Future<?> future : futureList) {
				Boolean computeResult = (Boolean) future.get();
				if (computeResult) {
					transfered++;
					if (transfered % 50 == 0 || transfered == contents.length) {
						logger.info("have transfered:" + transfered + " files");
					}
				} else {
					failed++;
					logger.error("failed transter:" + failed + " files");
				}
			}
			// 关闭线程池
			threadPool.shutdown();
			// 关闭FTPCient连接池
			ftpPool.close();
			// ****************
			if (filterStr != null) {
				logger.info("filter time:" + (endFileter - beginFilter) + " ms");
			}
			if (!overwrite) {
				logger.info("skip time:" + (endSkip - beginSkip) + " ms");
			}
			logger.info("total file count:" + contents.length);
			logger.info("total transtered: " + transfered + ",total failed:" + failed);
			if (!overwrite) {
				Log.info("total skiped files: " + skiped);
			}

		} else {// 若是文件，直接上传
			
			BufferedReader reader = null;
			FtpClientFactory facotry=new FtpClientFactory(ConfigUtils.ftpClientConfig);
			FTPClient ftpClient=null;
			InputStream in = null;
			try {
				Path path = fileStatus.getPath();
				if (!path.getName().contains("log")) {

				}
				reader = new BufferedReader(new FileReader(new File(path.toUri().getPath())));
				String str = null;
				
				ftpClient =facotry.makeObject();
				
				while ((str = reader.readLine()) != null) {
					String[] feilds=str.split("&");
					Path filePath=null;
					if(feilds.length==2&&feilds[1]!=""){
						filePath=new Path(feilds[1]);
						in = srcFS.open(filePath);
						boolean result=ftpClient.storeFile(dst.toString(), in);
						System.out.println(ftpClient.getReplyCode());
						if(result){
							logger.info(filePath.toString());
						}else{
							logger_failed.info(filePath.toString());
						}
					}else{
						continue;
					}
				
				}
			} catch (Exception e) {
				e.printStackTrace();
				
			} finally {
				in.close();
				reader.close();
				facotry.destroyObject(ftpClient);
			}

		}
		long end = System.currentTimeMillis();
		logger.info("finished transfer,total time:" + (end - start) / 1000 + "s");
		return true;
	}

}