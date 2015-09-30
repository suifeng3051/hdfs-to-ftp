package com.hdfstoftp.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool.impl.contrib.FTPClientPool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 具体执行上传文件动作的任务线程
 * @author heaven
 *
 */
public class UploadFileTask implements Callable<Boolean>{
	private static final Logger logger = LoggerFactory.getLogger("file");
	private static final Logger logger_failed = LoggerFactory.getLogger("failed");
	private FileSystem srcFS;
	private Path src;
	private Path dst;
	private boolean deleteSource;
	private FTPClientPool ftpClientPool;
	private boolean isRename=false;
	private String subDir;
	private int retryTimes;
	
	public UploadFileTask(FileSystem srcFS, Path src, Path dst, FTPClientPool ftpClientPool, boolean deleteSource,boolean isRename,String subDir,int retryTimes){
		this.srcFS=srcFS;
		this.src=src;
		this.dst=dst;
		this.ftpClientPool=ftpClientPool;
		this.deleteSource=deleteSource;
		this.isRename=isRename;
		this.subDir=subDir;
		this.retryTimes=retryTimes;
	}

	public Boolean call() throws Exception {
		boolean reply=false;
		InputStream in = null;
		try {
			try {
				//上传文件部分
				in = srcFS.open(src);
				FTPClient client= ftpClientPool.borrowObject();
				int tryedTimes=0;
				while(!reply&&(tryedTimes<=retryTimes)){
					reply=client.storeFile(dst.toString(),in);
					if(tryedTimes>0){
						Thread.sleep(100*tryedTimes);
						logger.warn("retryed times:"+tryedTimes+" return Info:"+client.getReplyString());
					}
					tryedTimes++;
				}
				ftpClientPool.returnObject(client);;
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
		
		if(reply){
			//若上传成功
			if(isRename){
				//把上传成功后的文件rename到日期子目录
				Path renamePath=new Path(src.getParent()+File.separator+subDir,src.getName());
				boolean result=srcFS.rename(src, renamePath);
				logger.info(renamePath.toString()+" "+result);
			}else{
				logger.info(src.toString());
			}
		}else{
			//若上传失败
			logger_failed.info("&"+src.toString());
		}
		if (deleteSource) {
			 srcFS.delete(src, true);
		} 
		return reply;
	}

}
