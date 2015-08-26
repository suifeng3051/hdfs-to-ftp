package org.apache.hadoop.contrib.ftp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author heaven
 *
 */
public class FtpClientUtil {
	/** logger */
	private static Logger logger = LoggerFactory.getLogger(FtpClientUtil.class);
	private static ThreadLocal<FTPClient> ftpClientThreadLocal;
	private static String host;
	private static int port;
	private static String username;
	private static String password;
	private static boolean binaryTransfer;
	private static String passiveMode;
	private static String encoding;
	private static int clientTimeout;
	public static  int threadNum;
	
	static {
		//加载配置文件
		ConfigUtils.loadConfig();
		ftpClientThreadLocal = new ThreadLocal<FTPClient>();
		host = ConfigUtils.getType("ftp.host");
		port = Integer.parseInt(ConfigUtils.getType("ftp.port"));
		username = ConfigUtils.getType("ftp.username");
		password = ConfigUtils.getType("ftp.password");
		clientTimeout = Integer.parseInt(ConfigUtils.getType("ftp.timeout"));
		passiveMode = ConfigUtils.getType("ftp.passivemode","false");
		encoding = ConfigUtils.getType("ftp.encoding","GBK");
		threadNum=Integer.valueOf(ConfigUtils.getType("transfer.threadnum","10"));
		logger.info("ftp.host:" + host);
		logger.info("ftp.port:" + port);
		logger.info("ftp.username:" + username);
		logger.info("ftp.password:" + password);
		logger.info("thread.num:" + threadNum);
		logger.info("ftp.timeout:" + clientTimeout);
		logger.info("ftp.passivemode:" + passiveMode);
		logger.info("ftp.encoding:" + encoding);
	}

	/**
	 * @description
	 * @return
	 * @throws FTPClientException
	 * @throws SocketException
	 * @throws IOException
	 */
	public static  FTPClient getFTPClient() throws SocketException, IOException, FTPClientException {
		if (ftpClientThreadLocal.get() != null && ftpClientThreadLocal.get().isConnected()) {
			return ftpClientThreadLocal.get();
		} else {
			FTPClient ftpClient=connect();
			ftpClientThreadLocal.set(ftpClient);
			return ftpClient;
		}
	}

	/**
	 * 创建一个ftpClient连接
	 * @return FTPClient
	 * @throws SocketException
	 * @throws IOException
	 * @throws FTPClientException
	 */
	private static FTPClient connect() throws SocketException, IOException, FTPClientException {
		FTPClient ftpClient = new FTPClient();
		ftpClient.setConnectTimeout(clientTimeout);
		ftpClient.connect(host, port);
		int reply = ftpClient.getReplyCode();
		if (FTPReply.isPositiveCompletion(reply)) {
			boolean result = ftpClient.login(username, password);
			if (!result) {
				throw new FTPClientException("ftpClient登陆失败! userName:" + username + " ; password:" + password);
			} 
			setFileType(ftpClient);
		} else {
			ftpClient.disconnect();
		}
		ftpClient.setBufferSize(1024);
		ftpClient.setControlEncoding(encoding);
		// 设置文件类型（二进制）
		ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
		if (passiveMode.equals("true")) {
			ftpClient.enterLocalPassiveMode();
		}
		return ftpClient;
	}

	/**
	 * @description
	 * @throws FTPClientException
	 * @throws IOException
	 */
	private static void setFileType(FTPClient ftpClient) throws IOException {
		if (binaryTransfer) {
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
		} else {
			ftpClient.setFileType(FTPClient.ASCII_FILE_TYPE);
		}
	}

	/**
	 * @description
	 * @throws FTPClientException
	 * @throws IOException
	 */
	public static void releaseFtpClient() throws IOException, FTPClientException {
		logger.info("disconnect ftpClient");
		FTPClient ftpClient = getFTPClient();
		ftpClient.logout();
		if (ftpClient.isConnected()) {
			ftpClient.disconnect();
			ftpClientThreadLocal.set(null);
		}
	}



	/**
	 * 
	 * @param remotePath
	 * @param localPath
	 * @return
	 * @throws IOException
	 * @throws FTPClientException
	 * @throws IOException
	 */
	public static boolean downloadToLocal(String remotePath, String localPath) throws IOException, IOException, FTPClientException {
		return downloadToLocal(remotePath, localPath, null);
	}

	/**
	 * 下载文件方法
	 * @param remotePath
	 * @param localPath
	 * @param fileNames
	 * @throws IOException
	 * @throws FTPClientException
	 * @throws IOException
	 */
	public static boolean downloadToLocal(String remotePath, String localPath, String[] fileNames) throws IOException, IOException, FTPClientException {

		FTPClient client = getFTPClient();
		client.changeWorkingDirectory(remotePath);
		FTPFile[] ftpList = client.listFiles(remotePath);
		boolean result = true;
		if (null == fileNames) {
			for (FTPFile f : ftpList) {
				if (f.getSize() > 0) {
					File file = new File(localPath);
					file.mkdirs();
					OutputStream out = new FileOutputStream(localPath + f.getName());
					result = client.retrieveFile(f.getName(), out);
					out.close();
					if (!result) {
						break;
					}
				}
			}
		} else {
			for (String fileName : fileNames) {
				File file = new File(localPath);
				file.mkdirs();
				OutputStream out = new FileOutputStream(localPath + File.separator + fileName);
				result = client.retrieveFile(fileName, out);
				out.close();
				if (!result) {
					break;
				}
			}
		}
		return result;
	}

	/**
	 * 获得文件大小方法
	 * @param client
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws FTPClientException
	 */
	public static int getRemoteFileSize(String fileName) throws IOException, FTPClientException {
		FTPClient client = getFTPClient();
		int size = 0;
		FTPFile[] ftpList = client.listFiles();
		for (FTPFile f : ftpList) {
			if (f.getName().equalsIgnoreCase(fileName)) {
				size = (int) f.getSize();
			}
		}
		return size;
	}

	
	/**
	 * @description 建文件夹方法
	 * @return boolean
	 * @throws SocketException
	 * @throws IOException
	 * @throws FTPClientException
	 */
	public static boolean mkdirs(String remoteDir) throws SocketException, IOException, FTPClientException {
		String[] dirs = remoteDir.split("/");
		String remotePath = ".";
		for (String dir : dirs) {
			if (!dir.equals(".") && null != dir) {
				remotePath = remotePath + File.separator + dir + File.separator;
				boolean result = getFTPClient().makeDirectory(remotePath);
				if (!result) {
					return result;
				}
			}
		}
		return true;
	}

	/**
	 * 获得指定路径下的文件名Map
	 * @param path
	 * @return  Map<String, String>
	 * @throws SocketException
	 * @throws IOException
	 * @throws FTPClientException
	 */
	public static Map<String, String> getFileNameMap(String path) throws SocketException, IOException, FTPClientException {
		Map<String, String> fileNameMap = new HashMap<String, String>();
		FTPClient client = getFTPClient();
		FTPFile[] files = client.listFiles(path);
		for (FTPFile file : files) {
			if (file.isFile()) {
				fileNameMap.put(file.getName(), "");
			}
		}
		return fileNameMap;
	}

	public static void main(String[] args) throws FTPClientException {
		try {
			getFileNameMap("/home/heaven/wanghouda");

		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	/**上传到ftp方法
	 * @param src
	 * @param in
	 * @return boolean
	 * @throws IOException
	 * @throws FTPClientException
	 */
	public static boolean uploadToRemote(String src, InputStream in) throws IOException, FTPClientException {
		FTPClient client = getFTPClient();
		boolean result = client.storeFile(src, in);
		in.close();
		return result;
	}

}
