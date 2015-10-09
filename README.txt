功能介绍

本工具的作用是将hdfs文件数据直接上传到ftp服务器，避免了传统方式先get数据到本地再上传到ftp。本工具可配合hdfs-over-ftp工具一起使用，实现两个集群之间的数据传递，hdfs-over-ftp工具地址：https://github.com/iponweb/hdfs-over-ftp

一、使用说明
1.hdfsToFtp-vxxx.jar 和conf目录拷贝到hadoop主机，jar文件和conf文件夹必须在同一级目录
2.修改conf目录下hdfs-on-ftp.properties配置
3.运行:hadoop jar hdfsToFtp.jar arg1 arg2 arg3 arg4

参数解释：
1.arg1:hdfs文件（目录）路径
2.arg2:ftp文件（目录）路径
3.arg3（可选）:从哪个时间点的文件开始（文件创建时间），可支持四种格式 1. yyyyMMdd：指定某一天的创建的文件 2. yyyyMMhhddHH:指定某一小时内创建的文件 3. yyyyMMhhddHHmmss~ :指定某个时间点之后创建的文件 4. 正则模式：[正则表达式]，注意正则表达式一定要用中括号括起来 5. 不填的话，默认所有的文件
4.arg4(可选)：是否覆盖参数，不填默认覆盖

二、举例说明

1.拷贝hdfs目录中的所有文件拷贝到ftp目录中 hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfix/anhui

2.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28号创建的文件 hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfix/anhui 20150628

3.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28号10点~11点之间创建的文件 hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui 2015062810

4.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28 8:45之后创建的文件 hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui 20150628084500~

5.拷贝hdfs目录中的文件到ftp目录，只拷贝6月28 8:45之后创建的文件，遇到相同文件名文件跳过不覆盖 hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui 20150628084500~ false

6.正则模式：

•上传接入区安徽省11号的详单数据到生产区hadoop jar hdfsToFtp-v1.3.jar /hdl/cdr/fix/trs/data/anhui /daas/bstl/cdrfix/anhui/20150711 [.*20150701.*]

•上传接入区安徽省11号的固网增值业务数据到生产区hadoop jar hdfsToFtp-v1.3.jar /hdl/cdr/fix/trs/data/anhui /daas/bstl/cdrfix/fix_vdr/anhui/20150711 [.*FIX_VDR_20150701.*]

三、效率

经测试，一个进程下速度约为60MB/s

四、配置文件中的相关参数

# ftp configure
ftp.host=192.168.29.131 
ftp.port=21
ftp.username=heaven
ftp.password=heaven
ftp.transfer.threadnum=10
ftp.timeout=5000
ftp.passivemode=true
ftp.encoding=GBK
#文件的传输类型，其值对应着FTPClient类的静态常量，2表示以字节流方式
ftp.transfer.file.type=2
#上传文件失败重试次数
ftp.retry.times=3
#是否对上传成功的文件进行rename
ftp.rename.uploaded=true
# hdfs configure


