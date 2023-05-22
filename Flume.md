# Flume

## 一、 Flume概述

==**Flume最主要的作用就是，实时读取服务器本地磁盘的数据，将数据写入到HDFS。**==

![image-20230521103247337](.assets/image-20230521103247337.png)



### 1. 基础架构

![image-20230521103332017](.assets/image-20230521103332017.png)

![image-20230521103402478](.assets/image-20230521103402478.png)

![image-20230521103425420](.assets/image-20230521103425420.png)





## 二、 Flume入门

### 1. 安装部署

![image-20230521104843515](.assets/image-20230521104843515.png)





### 2. Flume入门案例

#### 2.1 监控端口数据官方案例

##### 2.1.1 案例需求

使用 Flume 监听一个端口，==收集该端口数据==，并打印到控制台。

##### 2.1.2 需求分析

![image-20230521105227027](.assets/image-20230521105227027.png)

##### 2.1.3 实现步骤

1. 安装netcat工具

```shell
[atguigu@hadoop102 software]$ sudo yum install -y nc
```

2. 判断 44444 端口是否被占用

```shell
[atguigu@hadoop102 flume-telnet]$ sudo netstat -nlp | grep 44444
```

3. 创建 Flume Agent 配置文件 flume-netcat-logger.conf
4. 在 flume 目录下创建 job 文件夹并进入 job 文件夹

```she'l'l
[atguigu@hadoop102 flume]$ mkdir job
[atguigu@hadoop102 flume]$ cd job/
```

5. 在 job 文件夹下创建 Flume Agent 配置文件 flume-netcat-logger.conf

```shell
[atguigu@hadoop102 job]$ vim flume-netcat-logger.conf
```

6. 在 flume-netcat-logger.conf 文件中添加如下内容

```shell
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1
# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444
# Describe the sink
a1.sinks.k1.type = logger
# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100
# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

![image-20230521105813253](.assets/image-20230521105813253.png)

7. 开启 flume 监听端口

   1. 第一种写法

   ```shell
   [atguigu@hadoop102 flume]$ bin/flume-ng agent --conf conf/ --name 
   a1 --conf-file job/flume-netcat-logger.conf -
   Dflume.root.logger=INFO,console
   ```

   2. 第二种写法

   ```shell
   [atguigu@hadoop102 flume]$ bin/flume-ng agent -c conf/ -n a1 -f 
   job/flume-netcat-logger.conf -Dflume.root.logger=INFO,console
   ```

   参数说明： 

   - --conf/-c：表示配置文件存储在 conf/目录 

   - --name/-n：表示给 agent 起名为 a1 

   - --conf-file/-f：flume 本次启动读取的配置文件是在 job 文件夹下的 flume-telnet.conf 文件。 

   - -Dflume.root.logger=INFO,console ：-D 表示 flume 运行时动态修改 flume.root.logger 参数属性值，并将控制台日志打印级别设置为 INFO 级别。日志级别包括:log、info、warn、 error。

8. 使用 netcat 工具向本机的 44444 端口发送内容

```shell
[atguigu@hadoop102 ~]$ nc localhost 44444
hello 
atguigu
```

9. 在 Flume 监听页面观察接收数据情况
10. ![image-20230521110125388](.assets/image-20230521110125388.png)



#### 2.2 实时监控单个追加文件

##### 2.2.1 案例需求

==实时监控 Hive 日志，并上传到 HDFS 中==

##### 2.2.2 需求分析

![image-20230521111836344](.assets/image-20230521111836344.png)

##### 2.2.3 实现步骤

1. Flume 要想将数据输出到 HDFS，依赖 Hadoop 相关 jar 包

检查/etc/profile.d/my_env.sh 文件，确认 Hadoop 和 Java 环境变量配置正确

```shell
JAVA_HOME=/export/server/jdk
HADOOP_HOME=/export/server/hadoop
PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export PATH JAVA_HOME HADOOP_HOME
```

2. 创建 flume-file-hdfs.conf 文件

```shell
[atguigu@hadoop102 job]$ vim flume-file-hdfs.conf
```

添加如下内容：

```shell
# Name the components on this agent
a2.sources = r2
a2.sinks = k2
a2.channels = c2
# Describe/configure the source
a2.sources.r2.type = exec
a2.sources.r2.command = tail -F /opt/module/hive/logs/hive.log
# Describe the sink
a2.sinks.k2.type = hdfs
a2.sinks.k2.hdfs.path = hdfs://hadoop102:9820/flume/%Y%m%d/%H
#上传文件的前缀
a2.sinks.k2.hdfs.filePrefix = logs-
#是否按照时间滚动文件夹
a2.sinks.k2.hdfs.round = true
#多少时间单位创建一个新的文件夹
a2.sinks.k2.hdfs.roundValue = 1
#重新定义时间单位
a2.sinks.k2.hdfs.roundUnit = hour
#是否使用本地时间戳
a2.sinks.k2.hdfs.useLocalTimeStamp = true
#积攒多少个 Event 才 flush 到 HDFS 一次
a2.sinks.k2.hdfs.batchSize = 100
#设置文件类型，可支持压缩
a2.sinks.k2.hdfs.fileType = DataStream
#多久生成一个新的文件
a2.sinks.k2.hdfs.rollInterval = 60
#设置每个文件的滚动大小
a2.sinks.k2.hdfs.rollSize = 134217700
#文件的滚动与 Event 数量无关
a2.sinks.k2.hdfs.rollCount = 0
# Use a channel which buffers events in memory
a2.channels.c2.type = memory
a2.channels.c2.capacity = 1000
a2.channels.c2.transactionCapacity = 100
# Bind the source and sink to the channel
a2.sources.r2.channels = c2
a2.sinks.k2.channel = c2
```

注：对于所有与时间相关的转义序列，Event Header 中必须存在以 “timestamp”的 key（除非 hdfs.useLocalTimeStamp 设置为 true，此方法会使用 TimestampInterceptor 自 动添加 timestamp）。 ==a3.sinks.k3.hdfs.useLocalTimeStamp = **true**==

![image-20230521112542686](.assets/image-20230521112542686.png)

3. 运行 Flume

```shell
[atguigu@hadoop102 flume]$ bin/flume-ng agent --conf conf/ --name a2 --conf-file job/flume-file-hdfs.conf
```

4. 开启 Hadoop 和 Hive 并操作 Hive 产生日志

```shell
[atguigu@hadoop102 hadoop-2.7.2]$ sbin/start-dfs.sh
[atguigu@hadoop103 hadoop-2.7.2]$ sbin/start-yarn.sh
[atguigu@hadoop102 hive]$ bin/hive
hive (default)>
```

5. 在 HDFS 上查看文件



#### 2.3 实时监控目录下多个新文件







#### 2.4 实时监控目录下的多个追加文件 





