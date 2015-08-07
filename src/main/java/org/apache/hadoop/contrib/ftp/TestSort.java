package org.apache.hadoop.contrib.ftp;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;


public class TestSort {
	
	public static void main(String[] args) {
		String path=TestSort.class.getResource("/").getPath();
		System.out.println(path);
	}
	private static MyFile[] initMyFiles(int count) {
		 MyFile contents[]=new MyFile[count];
		for(int i=0;i<count;i++){
			String fileName=RandomStringUtils.random(10);
			long accesstime=RandomUtils.nextLong();
			MyFile myFile=new MyFile(fileName, accesstime);
			contents[i]=myFile;
		}
		return contents;
	}
	/**quick sort by access time */
	static MyFile[] quicksort(MyFile n[], int left, int right) {
        int dp;
        if (left < right) {
            dp = partition(n, left, right);
            quicksort(n, left, dp - 1);
            quicksort(n, dp + 1, right);
        }
        return n;
    }
 
    static int partition(MyFile content[], int left, int right) {
    	MyFile pivot = content[left];
        while (left < right) {
            while (left < right && content[right].getAccesstime() >= pivot.getAccesstime())
                right--;
            if (left < right)
            	content[left++] = content[right];
            while (left < right && content[left].getAccesstime() <= pivot.getAccesstime())
                left++;
            if (left < right)
            	content[right--] = content[left];
        }
        content[left] = pivot;
        return left;
    }
    public static class MyFile {
    	public MyFile(String fileName,long accesstime){
    		this.fileName=fileName;
    		this.accesstime=accesstime;
    	}
    	private long accesstime;
    	private String fileName;
    	public long getAccesstime() {
    		return accesstime;
    	}
    	public void setAccesstime(long accesstime) {
    		this.accesstime = accesstime;
    	}
    	public String getFileName() {
    		return fileName;
    	}
    	public void setFileName(String fileName) {
    		this.fileName = fileName;
    	}
    }
}
