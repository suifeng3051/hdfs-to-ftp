##功能介绍

本工具的作用是将hdfs文件数据直接上传到ftp服务器，避免了传统方式先get数据到本地再上传到ftp。本工具可配合hdfs-over-ftp工具一起使用，实现两个集群之间的数据传递，hdfs-over-ftp工具地址：https://github.com/iponweb/hdfs-over-ftp

##一、how to use

1. compile
    download the source code to your local, execute command ：`` mvn clean install``
2. modify the configure file
	open the compiled jar-file:hdfsToFtp-vxxx.jar,find and copy the conf dir to your local,and you can find the ``hdfs-on-ftp.properties`` in the conf dir
3. run the tool
	``hadoop jar hdfsToFtp.jar  <dir1> <dir2> [options] [option-value]``

###args explanation：

- ``<dir1>`` the source dir in your  file system(maybe hdfs)
- ``<dir2>`` the dest dir in your ftp server

###Options:

- -c 	your configure-file's absolute path		default value:	${your-app-path}/conf/hdfs-to-ftp.properties
- -d	filter parameter,only upload the files modified by a specified day,parameter format is: yyyyMMdd
- -h	filter parameter,only upload the files modified by a specified hour,parameter format is:yyyyMMhhddHH
- -t	filter parameter,only upload the files modified before a specified time,parameter format is: yyyyMMhhddHHmmss
- -r	filter regex ,use java regex to filter the files
- -o	overwrite flags,default	value:	true

##二、举例说明

1. 拷贝hdfs目录中的所有文件拷贝到ftp目录中 ``hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfix/anhui``

2. 拷贝hdfs目录中的文件到ftp目录，只拷贝6月28号创建的文件 ``hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfix/anhui -d 20150628``

3. 拷贝hdfs目录中的文件到ftp目录，只拷贝6月28号10点~11点之间创建的文件 ``hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui -h 2015062810``

4. 拷贝hdfs目录中的文件到ftp目录，只拷贝6月28 8:45之后创建的文件 ``hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui -t 20150628084500``

5. 拷贝hdfs目录中的文件到ftp目录，只拷贝6月28 8:45之后创建的文件，遇到相同文件名文件跳过不覆盖 ``hadoop jar hdfsToFtp.jar /itf/cdr/cdrfix/anhui /daas/nstl/cdrfox/anhui -t 20150628084500 -o false``

6. 正则模式,上传接入区安徽省11号的固网增值业务数据到生产区``hadoop jar hdfsToFtp-v1.3.jar /hdl/cdr/fix/trs/data/anhui /daas/bstl/cdrfix/fix_vdr/anhui/20150711 -r .*FIX_VDR_20150701.* ``

##三、效率

经测试，一个进程下速度约为60MB/s

##四、配置文件中的相关参数


    # ftp configure
    ftp.host=192.168.29.131 
    ftp.port=21
    ftp.username=heaven
    ftp.password=heaven
    ftp.timeout=5000
    ftp.passivemode=true
    ftp.encoding=GBK
    #文件的传输类型，其值对应着FTPClient类的静态常量，2表示以字节流方式
    ftp.transfer.file.type=2

    # application configure
    #上传文件失败重试次数
    app.retry.times=3
    #是否对上传成功的文件进行rename,默认把上传成功后的文件rename到uploaded子文件夹
    app.uploaded.isrename=true
    #上传的线程数量
    app.transfer.threadnum=10
    # hdfs configure





