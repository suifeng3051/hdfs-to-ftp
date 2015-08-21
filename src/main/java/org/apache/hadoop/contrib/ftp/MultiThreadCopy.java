package org.apache.hadoop.contrib.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 以多线程的方式拷贝hdfs文件到ftp服务器
 * @author heaven
 *
 */
public class MultiThreadCopy {
	/**
	 * 获取日志记录对象
	 */
	private static final Logger logger = LoggerFactory.getLogger(MultiThreadCopy.class);

	/**
	 * 主拷贝方法
	 * @param srcFS 文件系统
	 * @param src 源路径
	 * @param dst 目标路径
	 * @param queryStr 过滤字符串
	 * @param deleteSource 是否删除源文件
	 * @param overwrite 是否覆盖相同文件名文件
	 * @return boolean 
	 * @throws IOException
	 * @throws ParseException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws FTPClientException 
	 */
	public static boolean multiThreadCopy(FileSystem srcFS, Path src, Path dst, String filterStr, boolean deleteSource, boolean overwrite) throws IOException, ParseException, InterruptedException, ExecutionException, FTPClientException {
		long start = System.currentTimeMillis();
		//线程池
		ExecutorService threadPool = Executors.newFixedThreadPool(FtpClientUtil.threadNum);
		//获取ftp客户端
		FTPClient ftpClient = FtpClientUtil.getFTPClient();
		//获取目标路径
		String dstPath = dst.toUri().getPath();
		FileStatus fileStatus = srcFS.getFileStatus(src);
		if (fileStatus.isDirectory()) {//若是一个目录
			
			// 创建一个目标目录（如果乙存在默认不创建）
			ftpClient.makeDirectory(dstPath);
			//列出源目录中的文件信息
			FileStatus contents[] = srcFS.listStatus(src);
			long beginFilter = 0;
			long endFileter = 0;
			//若有过滤条件
			if (filterStr != null) {
				if (filterStr.startsWith("[")) {//若开头是"["，表示里面内容是正则表达式
					beginFilter = System.currentTimeMillis();
					contents = FileUtils.getFilterContents(filterStr, contents);
					endFileter = System.currentTimeMillis();
				} else {//若不是以"["开头，则表示时间段
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
			if (!overwrite) {//若不覆盖，获取源路径已存在的文件名列表
				beginSkip = System.currentTimeMillis();
				fileNameMap = FtpClientUtil.getFileNameMap(dstPath);
				endSkip = System.currentTimeMillis();
			}
			int skiped = 0;
			
			List<Future<?>> futureList=new ArrayList<Future<?>>();
			for (int i = 0; i < contents.length; i++) {
				if (!overwrite && fileNameMap.containsKey(contents[i].getPath().getName())) {
					//跳过已存在的文件
					skiped++;
					Log.info("skiped filename:" + contents[i].getPath().getName());
					continue;
				}
				//用多线程的方式拷贝源文件到目标路径
				Future<?> future=threadPool.submit(new CopyThread(srcFS, contents[i].getPath(), new Path(dst, contents[i].getPath().getName()), deleteSource));
				futureList.add(future);
			}
			int transfered = 0;
			int failed=0;
			for(Future<?> future:futureList){
				Object computeResult=future.get();
				if((Boolean)computeResult){
					transfered++;
					if (transfered % 50 == 0 || transfered == contents.length) {
						logger.info("have transfered:" + transfered + " files");
					}
				}else{
					failed++;
					logger.error("failed transter:" + failed + " files");
				}
			}
			threadPool.shutdown();
			//关闭ftpclient连接
			FtpClientUtil.releaseFtpClient();
			if (filterStr != null) {
				Log.info("filter time:" + (endFileter - beginFilter) + " ms");
			}
			if (!overwrite) {
				Log.info("skip time:" + (endSkip - beginSkip) + " ms");
			}
			Log.info("total file count:" + contents.length);
			Log.info("total transtered: " + transfered+",total failed:"+failed);
			if (!overwrite) {
				Log.info("total skiped files: " + skiped);
			}

		} else {// 若是文件，直接上传
			InputStream in = null;
			try {
				try {
					in = srcFS.open(src);
					FTPClient client= FtpClientUtil.getFTPClient();
					client.storeFile(dst.toString(),in);
					logger.info("transfered filename :" + dst.getName());
					if (deleteSource) {
						 srcFS.delete(src, true);
					} 
				} catch (IOException e) {
					IOUtils.closeStream(in);
					throw e;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally{	
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		long end = System.currentTimeMillis();
		logger.info("finished transfer,total time:" + (end - start) / 1000 + "s");
		return true;
	}

}
