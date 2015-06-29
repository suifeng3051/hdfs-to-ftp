package org.apache.hadoop.contrib.ftp;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsToFtp {
	private static Logger logger=LoggerFactory.getLogger(HdfsToFtp.class.getClass());


	static void printAndExit(String str) {
		logger.error(str);
		System.exit(1);
	}

	public static void main(String[] argv) throws IOException {
		
		if(argv==null||argv.length<2){
			printAndExit("输入参数不合法!");
		}
		String srcPath=argv[0];
		String dstPath=argv[1];
		String queryStr=null;
		boolean overwrite=false;
		if(argv.length>2){
			if(argv[2].length()==4){
				overwrite="true".equals(argv[2]);
			}else{
				queryStr=argv[2];
				if(argv.length>3){
					overwrite="true".equals(argv[3]);
				}
			}
			
		}
		Log.info("You input param is:"+argv[0]+" "+argv[1]+" "+(argv.length>2?argv[2]:"")+" "+(argv.length>3?argv[3]:""));
		Path hdfsPath = new Path(srcPath);
		Path ftpPath = new Path(dstPath);
		try {
			long start=System.currentTimeMillis();
			copyFromHDFSToFTP(hdfsPath,ftpPath,queryStr,overwrite);
			long end=System.currentTimeMillis();
			Log.info("finished transfer,total time:"+(end-start)/1000+"s");
		} catch (URISyntaxException e) {
			e.printStackTrace();
			Log.info(e.getMessage());
		} catch(Exception e){
			e.printStackTrace();
			Log.info(e.getMessage());
		}
		
	}

	private static void copyFromHDFSToFTP(Path hdfsPath, Path ftpPath, String queryStr,boolean overwrite) throws IOException, URISyntaxException, ParseException {
		//获取hdfs文件系统
		Configuration conf = new Configuration();
		FileSystem hdfsSystem = FileSystem.get(conf);
		FileUtils.copy(hdfsSystem, hdfsPath,ftpPath,queryStr, false, true, conf);
		
	}
}