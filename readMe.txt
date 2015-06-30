一、使用说明
1.解压
2.把文件夹中的hdfsToFtp.jar 和conf目录拷贝到hadoop主机，jar文件和conf文件夹必须在同一级目录
3.修改conf目录下hdfs-on-ftp.properties配置
4.运行:hadoop jar hdfsToFtp.jar arg1 arg2 arg3 arg4

参数解释：
arg1:hdfs文件（目录）路径
arg2:ftp文件（目录）路径
arg3（可选）:从哪个时间点的文件开始（文件创建时间），可支持三种格式
  1.yyyyMMdd：指定某一天的创建的文件
  2.yyyyMMhhddHH:指定某一小时内创建的文件
  3.yyyyMMhhddHHmmss~ :指定某个时间点之后创建的文件
  4.不填的话，默认所有的文件
arg4(可选)：是否覆盖参数，不填默认覆盖

二、举例说明：

1.拷贝hdfs目录中的所有文件拷贝到ftp目录中
  hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfix/anhui
 
2.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28号创建的文件
  hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfix/anhui 20150628

3.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28号10点~11点之间创建的文件
  hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui 2015062810

4.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28 8:45之后创建的文件
  hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui 20150628084500~

5.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28 8:45之后创建的文件，遇到相同文件名文件跳过不覆盖
  hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui 20150628084500~ false

三、效率
经测试，一个进程下速度约为60MB/s
