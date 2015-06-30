package org.apache.hadoop.contrib.ftp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FtpClientUtil {
	/** logger */
	private static Logger logger=LoggerFactory.getLogger(HdfsToFtp.class.getClass());
	private static ThreadLocal<FTPClient> ftpClientThreadLocal;
	private static String host;
	private static int port;
	private static String username;
	private static String password;
	private static boolean binaryTransfer;
	//private static boolean passiveMode;
	//private static String encoding;
	private static int clientTimeout;
	private static List<String> listFileNames;
	// private static String filePathOfFtpserver;

	static {
		ConfigUtils.loadConfig();
		ftpClientThreadLocal = new ThreadLocal<FTPClient>();
		host = ConfigUtils.getType("ftp.host");
		port = Integer.parseInt(ConfigUtils.getType("ftp.port"));
		username = ConfigUtils.getType("ftp.username");
		password = ConfigUtils.getType("ftp.password");
		/*
		 * binaryTransfer = Boolean.parseBoolean(ConfigUtils
		 * .getType("ftp.binaryTransfer")); passiveMode =
		 * Boolean.parseBoolean(ConfigUtils .getType("ftp.passiveMode"));
		 * encoding = ConfigUtils.getType("ftp.encoding");
		 */
		clientTimeout = Integer.parseInt(ConfigUtils
				.getType("ftp.timeout"));
		listFileNames = new ArrayList<String>();
		logger.info("ftp.host:"+host);
		logger.info("ftp.port:"+port);
		logger.info("ftp.username:"+username);
		logger.info("ftp.password:"+password);
		logger.info("ftp.timeout:"+clientTimeout);
	}

	/**
	 * @description
	 * @return
	 * @throws FTPClientException
	 * @throws SocketException
	 * @throws IOException
	 */
	public static FTPClient getFTPClient() throws SocketException, IOException {
		if (ftpClientThreadLocal.get() != null
				&& ftpClientThreadLocal.get().isConnected()) {
			return ftpClientThreadLocal.get();
		} else {
			FTPClient ftpClient = new FTPClient();
			//ftpClient.setControlEncoding(encoding);
			ftpClient.setConnectTimeout(clientTimeout);
			ftpClient.connect(host, port);
			int reply = ftpClient.getReplyCode();
			if (FTPReply.isPositiveCompletion(reply)) {
				ftpClient.login(username, password);
				setFileType(ftpClient);
			} else {
				ftpClient.disconnect();
			}
			 ftpClient.setBufferSize(1024); 
	         ftpClient.setControlEncoding("GBK" ); 
	            //设置文件类型（二进制） 
	         ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE); 
			/*if (passiveMode) {
				ftpClient.enterLocalPassiveMode();
			}*/
			ftpClientThreadLocal.set(ftpClient);
			return ftpClient;
		}
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
	public static void disconnect() throws IOException {
		FTPClient ftpClient = getFTPClient();
		ftpClient.logout();
		if (ftpClient.isConnected()) {
			ftpClient.disconnect();
			ftpClient = null;
			ftpClientThreadLocal.set(ftpClient);
		}
	}

	/**
	 * @description 
	 *             
	 * @param delFiles
	 * @return
	 * @throws FTPClientException
	 * @throws IOException
	 */
	public static boolean deleteRemoteFiles(String[] delFiles)
			throws IOException, FTPClientException {
		List<String> list = listNames();
		for (String filename : delFiles) {
			for (Iterator<String> it = list.iterator(); it.hasNext();) {
				String filepath = it.next();
				if (filepath.contains(filename)) {
					boolean result = getFTPClient().deleteFile(filepath);
					if (!result) {
						return result;
					}
				}
			}
		}
		return true;
	}

	/**
	 * @description
	 * @return
	 * @throws FTPClientException
	 * @throws IOException
	 */
	public static List<String> listNames() throws IOException,
			FTPClientException {
		return listNames(null);
	}

	public static List<String> listNames(String remotePath) throws IOException,
			FTPClientException {
		return listNames(remotePath, true);
	}

	/**
	 * @description
	 * @param remotePath
	 * @param autoClose
	 * @throws FTPClientException
	 * @throws IOException
	 */
	public static List<String> listNames(String remotePath,
			boolean containSubdirectory) throws IOException, FTPClientException {
		if (null == remotePath) {
			remotePath = "." + File.separator;
		}
		try {
			FTPFile[] files = getFTPClient().listFiles(remotePath);
			if (files.length < 3) {
				return listFileNames;
			}
			for (FTPFile file : files) {
				if (!file.getName().equals(".") && !file.getName().equals("..")) {
					if (file.isFile()) {
						listFileNames
								.add("." + File.separator + file.getName());
					} else {
						listNames2(
								remotePath + file.getName() + File.separator,
								containSubdirectory);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new FTPClientException(
					"",
					e);
		}
		return listFileNames;
	}


	private static void listNames2(String remotePath,
			boolean containSubdirectory) throws FTPClientException {
		try {
			FTPClient client = getFTPClient();
			client.changeWorkingDirectory(remotePath);
			FTPFile[] files = client.listFiles(remotePath);
			if (files.length < 3) {
				return;
			}
			for (FTPFile file : files) {
				if (!file.equals(".") && !file.equals("..")) {
					if (file.isFile()) {
						listFileNames.add(remotePath + file.getName());
					}
					if (file.isDirectory() && (!".".equals(file.getName()))
							&& (!"..".equals(file.getName()))) {
						String path = remotePath + file.getName()
								+ File.separator;
						listNames2(path, containSubdirectory);
					}
				}
			}
		} catch (IOException e) {
			throw new FTPClientException(
					"",
					e);
		}
	}

	/**
	 * 
	 * @param remotePath
	 *            \u8FDC\u7A0B\u8DEF\u5F84
	 * @param fileName
	 *            \u8981\u4E0A\u4F20\u7684\u6587\u4EF6\u540D
	 * @param localInputStream
	 *            \u672C\u5730InputStream\u6D41
	 * @return
	 * @throws IOException
	 * @throws FTPClientException
	 */
	public static boolean uploadToRemote(String remotePath, String fileName,
			InputStream localInputStream) throws IOException,
			FTPClientException {
		FTPClient client = getFTPClient();
		int reply;
		reply = client.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			client.disconnect();
		}
		client.makeDirectory(remotePath);
		client.changeWorkingDirectory(remotePath);
		boolean result = client.storeFile(fileName,
				localInputStream);
		localInputStream.close();
		return result;
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
	public static boolean downloadToLocal(String remotePath, String localPath)
			throws IOException, IOException {
		return downloadToLocal(remotePath, localPath, null);
	}

	/**
	 * 
	 * @param remotePath
	 * @param localPath
	 * @param fileNames
	 * @throws IOException
	 * @throws FTPClientException
	 * @throws IOException
	 */
	public static boolean downloadToLocal(String remotePath, String localPath,
			String[] fileNames) throws IOException, IOException {
		
		FTPClient client = getFTPClient();
		client.changeWorkingDirectory(remotePath);
		FTPFile[] ftpList = client.listFiles(remotePath);
		boolean result = true;
		if (null == fileNames) {
			for (FTPFile f : ftpList) {
				if (f.getSize() > 0) {
					File file = new File(localPath);
					file.mkdirs();
					OutputStream out = new FileOutputStream(localPath
							+ f.getName());
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
				OutputStream out = new FileOutputStream(localPath
						+ File.separator + fileName);
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
	 * @param client
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws FTPClientException
	 */
	public static int getRemoteFileSize(String fileName) throws IOException,
			FTPClientException {
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
	 * 
	 * @param filename
	 * @param localPath
	 * @return
	 * @throws Exception
	 */
	public static boolean downloadToLocal2(String filename, String localPath)
			throws Exception {
		List<String> list = listNames();
		OutputStream out;
		try {
			for (Iterator<String> it = list.iterator(); it.hasNext();) {
				String filepath = it.next();
				if (filepath.contains(filename)) {
					String remoteFilePath = filepath.substring(1,
							filepath.length());
					File file = new File(localPath + remoteFilePath);
					new File(file.getParent()).mkdirs();
					out = new FileOutputStream(localPath + remoteFilePath);
					getFTPClient().retrieveFile(filepath, out); 
					out.close();
				}
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * @description 
	 * @return
	 * @throws SocketException
	 * @throws IOException
	 * @throws FTPClientException
	 */
	public static boolean mkdirs(String remoteDir) throws SocketException,
			IOException, FTPClientException {
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
	public static Map<String, String> getFileNameMap(String path) throws SocketException, IOException{
		Map<String, String> fileNameMap=new HashMap<String, String>();
		FTPClient client= getFTPClient();
		FTPFile[] files=client.listFiles(path);
		for(FTPFile file:files){
			if(file.isFile()){
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

	public static boolean uploadToRemote(String src, InputStream in) throws IOException {
		FTPClient client= getFTPClient();
		boolean result = client.storeFile(src,in);
		in.close();
		return result;
		
	}

}

