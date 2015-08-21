package org.apache.hadoop.contrib.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyThread implements Callable<Boolean>{
	private static final Logger logger = LoggerFactory.getLogger(CopyThread.class);
	private FileSystem srcFS;
	private Path src;
	private Path dst;
	private boolean deleteSource;
	
	
	public CopyThread(FileSystem srcFS, Path src, Path dst, boolean deleteSource){
		this.srcFS=srcFS;
		this.src=src;
		this.dst=dst;
		this.deleteSource=deleteSource;
	}

	public Boolean call() throws Exception {
		boolean result=false;
		InputStream in = null;
		try {
			try {
				in = srcFS.open(src);
				FTPClient client= FtpClientUtil.getFTPClient();
				result=client.storeFile(dst.toString(),in);
				logger.info(Thread.currentThread().getName()+ " transfered filename :" + dst.getName());
				
				if (deleteSource) {
					 srcFS.delete(src, true);
				} 
			} catch (IOException e) {
				IOUtils.closeStream(in);
				throw e;
			} catch (FTPClientException e) {
				e.printStackTrace();
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
		return result;
	}

}
