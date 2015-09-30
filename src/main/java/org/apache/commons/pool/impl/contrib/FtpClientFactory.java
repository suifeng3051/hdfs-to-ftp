package org.apache.commons.pool.impl.contrib;

import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.pool.PoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hdfstoftp.util.FTPClientException;

/**
 * FTPClient工厂类，通过FTPClient工厂提供FTPClient实例的创建和销毁
 * @author heaven
 */
public class FtpClientFactory implements PoolableObjectFactory<FTPClient> {
	private static Logger logger = LoggerFactory.getLogger("file");
	private  FTPClientConfigure config;
	public FtpClientFactory(FTPClientConfigure config){
		this.config=config;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.commons.pool.PoolableObjectFactory#makeObject()
	 */
	public FTPClient makeObject() throws Exception {
		FTPClient ftpClient = new FTPClient();
		ftpClient.setConnectTimeout(config.getClientTimeout());
		try {
			ftpClient.connect(config.getHost(), config.getPort());
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				logger.warn("FTPServer refused connection");
				return null;
			}
			boolean result = ftpClient.login(config.getUsername(), config.getPassword());
			if (!result) {
				throw new FTPClientException("ftpClient登陆失败! userName:" + config.getUsername() + " ; password:" + config.getPassword());
			}
			ftpClient.setFileType(config.getTransferFileType());
			ftpClient.setBufferSize(1024);
			ftpClient.setControlEncoding(config.getEncoding());
			if (config.getPassiveMode().equals("true")) {
				ftpClient.enterLocalPassiveMode();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (FTPClientException e) {
			e.printStackTrace();
		}
		return ftpClient;
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.pool.PoolableObjectFactory#destroyObject(java.lang.Object)
	 */
	public void destroyObject(FTPClient ftpClient) throws Exception {
		try {
			if (ftpClient != null && ftpClient.isConnected()) {
				ftpClient.logout();
			}
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			// 注意,一定要在finally代码中断开连接，否则会导致占用ftp连接情况
			try {
				ftpClient.disconnect();
			} catch (IOException io) {
				io.printStackTrace();
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.pool.PoolableObjectFactory#validateObject(java.lang.Object)
	 */
	public boolean validateObject(FTPClient ftpClient) {
		try {
			return ftpClient.sendNoOp();
		} catch (IOException e) {
			throw new RuntimeException("Failed to validate client: " + e, e);
		}
	}

	public void activateObject(FTPClient ftpClient) throws Exception {
	}

	public void passivateObject(FTPClient ftpClient) throws Exception {

	}

}
