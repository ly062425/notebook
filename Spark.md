# Spark（RDD）

## 一、Spark基础入门 （大规模数据处理）

### 1. 简介

![image-20230411094133548](.assets/image-20230411094133548.png)

![image-20230411094335113](.assets/image-20230411094335113.png)



==**Spark和Hadoop的区别**==（spark运算比hadoop快100倍）

![image-20230411094818543](.assets/image-20230411094818543.png)

![image-20230411095104561](.assets/image-20230411095104561.png)

![image-20230411095814811](.assets/image-20230411095814811.png)



==**运行模式**==

![image-20230411100031561](.assets/image-20230411100031561.png)

![image-20230411100412209](.assets/image-20230411100412209.png)





### 2. Local模式运行

==**基本原理**==

![image-20230411100932687](.assets/image-20230411100932687.png)





### 3. StandAlone模式

==**基本架构**==

![image-20230411140622998](.assets/image-20230411140622998.png)



![image-20230411143451170](.assets/image-20230411143451170.png)

![image-20230411144128461](.assets/image-20230411144128461.png)





### 4. StandAlone HA模式

==**运行原理**==

![image-20230412175144593](.assets/image-20230412175144593.p

![image-20230412175347331](.assets/image-20230412175347331.png)





### 5. Spark On Yarn 

```shell
bin/pyspark --master yarn
```



#### 	5.1 本质

![image-20230412181014868](.assets/image-20230412181014868.png)



#### 	5.2 Cluster模式（推荐）

![image-20230412192457418](.assets/image-20230412192457418.png)

![image-20230412212355339](.assets/image-20230412212355339.png)

![image-20230412213226420](.assets/image-20230412213226420.png)

![image-20230412213237342](.assets/image-20230412213237342.png)



#### 	5.3 Client模式

![image-20230412192516306](.assets/image-20230412192516306.png)

![image-20230412212409244](.assets/image-20230412212409244.png)

执行步骤：

![image-20230412213137389](.assets/image-20230412213137389.png)

![image-20230412213211292](.assets/image-20230412213211292.png)



==**区别**==

![image-20230412192552876](.assets/image-20230412192552876.png)



#### 5.4 连接时遇到的问题

Hadoop集群安全模式退出失败问题处理，Safe mode is ON。hdfs dfsadmin -safemode leave 或 forceExit

```
#正常退出安全模式
hdfs dfsadmin -safemode leave
#强制退出安全模式
hdfs dfsadmin -safemode forceExit
#删除损坏的block
hdfs fsck / -delete
```





### 6. 框架和类库

![image-20230412213730601](.assets/image-20230412213730601.png)

![image-20230413142303772](.assets/image-20230413142303772.png)





### 7. 本机配置pyspark

![image-20230413210027660](.assets/image-20230413210027660.png)

```python
# coding:utf-8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("wordCountHelloWorld")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)
```



==**WordCount原理分析**==

![image-20230413213154646](.assets/image-20230413213154646.png)

```python
	#wordCount单词计数，读取hdfs的words.txt文件
    #读取文件
    #file_rdd = sc.textFile("hdfs://node1:8020/root/input/words.txt")
    file_rdd = sc.textFile("../data/input/words.txt")

    #将单词进行切割,得到集合对象
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    #将单词转换为元组对象，key是单词，value是数字
    words_rdd_map = words_rdd.map(lambda x: (x, 1))

    #将元组的value按照key分组，对value聚合
    result_rdd = words_rdd_map.reduceByKey(lambda a, b: a + b)

    #通过clooect方法收集RDD数据
    print(result_rdd.collect())
```





### 8. 分布式代码执行分析

![image-20230413215042336](.assets/image-20230413215042336.png)



==**执行过程**==

![image-20230413215435756](.assets/image-20230413215435756.png)

![image-20230413215448746](.assets/image-20230413215448746.png)

![image-20230413215518723](.assets/image-20230413215518723.png)



#### 	Python On Spark执行原理

![image-20230413215636703](.assets/image-20230413215636703.png)

![image-20230413215652312](.assets/image-20230413215652312.png)







## 二、Spark Core

### 1. RDD详解

#### 	1.1 为什么需要RDD

![image-20230413220247793](.assets/image-20230413220247793.png)

#### 	1.2 什么是RDD

![image-20230413220327103](.assets/image-20230413220327103.png)

#### 		1.3 RDD五大特性

1. RDD是有分区的（==**数据存储的最小单位**==）
2. RDD的方法会作用在==**所有的分区**==上
3. RDD之间具有**==依赖==关系**
4. Key-Value型的RDD可以有分区器（Hash分区规则）
5. RDD的分区规划，会尽量靠近数据所在的服务器（走本地读取，避免网络读取）





### 2. RDD编程入门

#### 	2.1 程序执行入口SparkContext对象

![image-20230414135742971](.assets/image-20230414135742971.png)



#### 	2.2 RDD的创建

![image-20230414135844820](.assets/image-20230414135844820.png)



==**并行化创建**==

![image-20230414135929263](.assets/image-20230414135929263.png)

```python
#获取RDD分区数
rdd.getNumPartitions()
#collect方法，是将Rdd中每个分区的数据发送到Driver中，形成一个python list对象
#分布式转本地集合
rdd.collect()
```



==**读取文件创建**==

![image-20230414141456526](.assets/image-20230414141456526.png)

![image-20230414141513091](.assets/image-20230414141513091.png)



#### 	2.3 RDD算子

![image-20230414142725158](.assets/image-20230414142725158.png)

![image-20230414164556130](.assets/image-20230414164556130.png)

![image-20230414164613736](.assets/image-20230414164613736.png)



#### 		2.4 常用Transformation算子（运算）

##### 				2.4.1 map算子

![image-20230414164844764](.assets/image-20230414164844764.png)

```python
#coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == "__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)

    rdd=sc.parallelize([1,2,3,4,5],3)

    def add(data):
        return data*10

    print(rdd.map(add).collect())

    #匿名函数
    print(rdd.map(lambda data: data*10).collect())

    print(rdd.getNumPartitions())
```

​	

##### 				2.4.2  flatMap算子（解除嵌套）

![image-20230414164912779](.assets/image-20230414164912779.png)

##### 				2.4.3  reduceByKey算子

![image-20230414171931149](.assets/image-20230414171931149.png)

##### 				2.4.4 mapValues算子

![image-20230414172607612](.assets/image-20230414172607612.png)

##### 				2.4.5 groupBy算子

![image-20230414172921161](.assets/image-20230414172921161.png)

##### 				2.4.6 Filter算子

![image-20230414221227112](.assets/image-20230414221227112.png)

##### 				2.4.7 distinct算子

![image-20230414221245840](.assets/image-20230414221245840.png)

##### 				2.4.8 union算子（并）

![image-20230414221317277](.assets/image-20230414221317277.png)

##### 				2.4.9 join算子（补）

![image-20230414221328628](.assets/image-20230414221328628.png)

##### 				2.4.10 intersection算子（交）

![image-20230414221339948](.assets/image-20230414221339948.png)

##### 				2.4.11 glom算子

![image-20230414221355321](.assets/image-20230414221355321.png)

##### 				2.4.12 groupByKey算子

![image-20230414221406138](.assets/image-20230414221406138.png)

##### 				2.4.13 sortBy算子

![image-20230414221415923](.assets/image-20230414221415923.png)

##### 				2.4.14 sortByKey算子

![image-20230414221429711](.assets/image-20230414221429711.png)



#### 	2.5 常用Action算子（返回值）

##### 				2.5.1 countByKey算子

![image-20230414221519338](.assets/image-20230414221519338.png)

##### 				2.5.2 collect算子

![image-20230414221530661](.assets/image-20230414221530661.png)

##### 				2.5.3 reduce算子

![image-20230414221542153](.assets/image-20230414221542153.png)

##### 				2.5.4 fold算子

![image-20230414221551737](.assets/image-20230414221551737.png)

##### 				2.5.5 first算子

![image-20230414221606990](.assets/image-20230414221606990.png)

##### 				2.5.6 take算子

![image-20230414221614360](.assets/image-20230414221614360.png)

##### 				2.5.7 top算子

![image-20230414221623356](.assets/image-20230414221623356.png)

##### 				2.5.8 count算子

![image-20230414221636415](.assets/image-20230414221636415.png)

##### 				2.5.9 takeSample算子

![image-20230414221651261](.assets/image-20230414221651261.png)

##### 				2.5.10 takeOrdered算子

![image-20230414221701158](.assets/image-20230414221701158.png)

##### 				2.5.11 foreach算子（分区Executor执行）

![image-20230414221711028](.assets/image-20230414221711028.png)

##### 				2.5.12 saveAsTextFile算子（分区Executor执行）

![image-20230414221721688](.assets/image-20230414221721688.png)

![image-20230414221741312](.assets/image-20230414221741312.png)



#### 	2.6 分区操作算子

##### 				2.6.1 mapPartition算子

![image-20230414221928669](.assets/image-20230414221928669.png)

##### 				2.6.2 foreachPartition算子

![image-20230414222324540](.assets/image-20230414222324540.png)

##### 				2.6.3 partitionBy算子

![image-20230414222315403](.assets/image-20230414222315403.png)

![image-20230414222403263](.assets/image-20230414222403263.png)				

##### 				2.6.4 repartition算子

![image-20230414222302918](.assets/image-20230414222302918.png)

##### 				2.6.5 coalesce算子

![image-20230414222255422](.assets/image-20230414222255422.png)

##### 				2.6.6 mapValues算子

![image-20230414222247558](.assets/image-20230414222247558.png)

##### 				2.6.7 join算子

![image-20230414222137597](.assets/image-20230414222137597.png)



#### 	2.7 ==面试题：groupByKey和reduceByKey的区别==

![image-20230418155111769](.assets/image-20230418155111769.png)

**groupByKey**

![image-20230418155209766](.assets/image-20230418155209766.png)

**redeuceByKey**

![image-20230418155235355](.assets/image-20230418155235355.png)





### 3. RDD持久化

#### 3.1 过程数据

![image-20230418220129131](.assets/image-20230418220129131.png)



#### 3.2 RDD缓存（设计上不安全）

![image-20230418220612843](.assets/image-20230418220612843.png)

![image-20230418220653252](.assets/image-20230418220653252.png)



#### 3.3 CheckPoint（设计上安全）

![image-20230418221003182](.assets/image-20230418221003182.png)

![image-20230418221018470](.assets/image-20230418221018470.png)





### 4. Spark案例练习

#### 4.1 搜索引擎日志分析

![image-20230418221636769](.assets/image-20230418221636769.png)

**main.py**

```python
# coding:utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs import context_jieba,filter_words,append_words,extract_user_and_word
from operator import add

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #1. 读取数据文件
    file_rdd = sc.textFile("../../data/input/SogouQ.txt")

    #2. 对数据切分 \t
    split_rdd = file_rdd.map(lambda x: x.split("\t"))

    #3. split_rdd作为基础rdd会被多次使用
    split_rdd.persist(StorageLevel.DISK_ONLY)

    #TODO: 需求1 ：用户搜索的关键词分析
    #将搜索内容取出
    #print(split_rdd.takeSample(True, 3))
    context_rdd = split_rdd.map(lambda x: x[2])

    #对搜索的内容分析
    words_rdd = context_rdd.flatMap(context_jieba)
    
    #print(words_rdd.collect())
    filtered_rdd = words_rdd.filter(filter_words)

    #关键词转换
    final_words_rdd = filtered_rdd.map(append_words)

    #单词计数/分组/排序
    result1=final_words_rdd.reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).\
        take(5)

    print("需求1结果: ",result1)


    # TODO: 需求2 ：用户和关键词组合分析
    user_content_rdd=split_rdd.map(lambda x:(x[1],x[2]))
    #对用户搜索内容切分和id组合
    user_word_rdd = user_content_rdd.flatMap(extract_user_and_word())

    #对内容分组/排序/聚合
    result2 = user_word_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(
        5)
    print("需求2: ",result2)


    # TODO: 需求3 ：搜索时间段分析
    time_rdd = split_rdd.map(lambda x: x[0])

    #对时间进行处理只保留小时
    hour_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))

    result3 = hour_rdd.reduceByKey(add).sortBy(lambda x: x[1], ascending=False, numPartitions=1).collect()

    print("需求3: ",result3)
```



**defs.py**

```python
#coding:utf8

import jieba

def context_jieba(data):
    '''通过jieba分词工具 进行分词操作'''

    seg = jieba.cut_for_search(data)
    l=list()
    for word in seg:
        l.append(word)
    return l

def filter_words(data):
    '''过滤不要的字'''
    return data not in ['谷','帮','客']

def append_words(data):
    '''修订某关键词'''
    if data=='传智播':data='传智播客'
    if data == '院校': data = '院校帮'
    if data == '博学': data = '博学谷'
    return (data,1)

def extract_user_and_word(data):
    '''传入数据是 元组'''
    user_id=data[0]
    content=data[1]
    #对content分词
    words=context_jieba(content)

    return_list=list()
    for word in words:
        if filter_words(word):
            return_list.append((user_id+"_"+append_words(word)[0],1))
    return return_list

```



#### 4.2 提交到集群运行

![image-20230419181331445](.assets/image-20230419181331445.png)





### 5. 共享变量

#### 5.1 广播变量

![image-20230419210852916](.assets/image-20230419210852916.png)

==**使用方式**==

![image-20230419211035100](.assets/image-20230419211035100.png)



#### 5.2 累加器

![image-20230419211618786](.assets/image-20230419211618786.png)

![image-20230419211832196](.assets/image-20230419211832196.png)



#### 5.3 综合案例

![image-20230419214235764](.assets/image-20230419214235764.png)





### 6. ==Spark内核调度==

#### 6.1 DAG

![image-20230419221456246](.assets/image-20230419221456246.png)

![image-20230420201215118](.assets/image-20230420201215118.png)

==**一个Action会产生一个Job**==



#### 6.2 DAG的宽窄依赖和阶段划分

![image-20230420202226753](.assets/image-20230420202226753.png)

==**窄依赖**== （父亲全部给一个子）

![image-20230420202259349](.assets/image-20230420202259349.png)

==**宽依赖**== （父亲全部分给多个子）

![image-20230420202320444](.assets/image-20230420202320444.png)

==**阶段划分**==

![image-20230420202938583](.assets/image-20230420202938583.png)



#### 6.3 内存迭代计算

![image-20230420203650473](.assets/image-20230420203650473.png)



==**面试题1：**==Spark是怎么做内存计算的？DAG的作用？Stage阶段划分的作用？

![image-20230420203829098](.assets/image-20230420203829098.png)

==**面试题2：**==Spark为什么比MapReduce快

![image-20230420203859859](.assets/image-20230420203859859.png)



#### 6.4 Spark并行度（并行能力）

==**在同一时间内，有多少个task（分区）在同时运行**==

![image-20230420204313342](.assets/image-20230420204313342.png)

![image-20230420204348255](.assets/image-20230420204348255.png)

![image-20230420204445024](.assets/image-20230420204445024.png)



#### 6.5 Spark任务调度

![image-20230420204630450](.assets/image-20230420204630450.png)

![image-20230420204647072](.assets/image-20230420204647072.png)

![image-20230420204805864](.assets/image-20230420204805864.png)

 

#### 6.6 Spark运行层级梳理

![image-20230420205459460](.assets/image-20230420205459460.png)







## 三、SparkSQL（运行SQL处理数据）

### 1. SparkSQL入门 （结构化数据处理）

![image-20230420210522945](.assets/image-20230420210522945.png)





### 2. SparkSQL概述

#### 2.1 SparkSQL和Hive异同

![image-20230420210751984](.assets/image-20230420210751984.png)



#### 2.2 SparkSQL数据抽象

![image-20230421082653117](.assets/image-20230421082653117.png)



#### 2.3 DataFrame概述

![image-20230421082937540](.assets/image-20230421082937540.png)



#### 2.4 ==SparkSession对象==

![image-20230421083110914](.assets/image-20230421083110914.png)

![image-20230421084609851](.assets/image-20230421084609851.png)





### 3. DataFrame入门和操作

#### 3.1 DataFrame组成

![image-20230421084904655](.assets/image-20230421084904655.png)

==**例子：**==

![image-20230421085057180](.assets/image-20230421085057180.png)



#### 3.2 ==RDD构建==DataFrame （createDataFrame）

##### 		3.2.1 基于RDD方式（转换内部存储结构为二维表结构）

```python
# coding:utf8

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    #基于RDD转换DataFrame
    rdd=sc.textFile("../data/input/sql/people.txt").\
        map(lambda x:x.split(",")).\
        map(lambda x:(x[0],int(x[1])))

    #构建DataFrame对象
    #参数1 被转换的rdd
    #参数2 指定列明通过list方式
    df=spark.createDataFrame(rdd,schema=['name','age'])

    #表结构
    df.printSchema()

    #打印数据
    #参数1 展示条数
    #参数2 是否截断列
    df.show(20,False)

    #将DF对象转换成临时视图表供sql查询
    df.createOrReplaceTempView("people")
    spark.sql("select * from people where age<30").show()

```

##### 		3.2.2 基于RDD方式（通过StructType对象定义DataFrame的表结构转换）

```python
# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,IntegerType

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    #基于RDD转换DataFrame
    rdd=sc.textFile("../data/input/sql/people.txt").\
        map(lambda x:x.split(",")).\
        map(lambda x:(x[0],int(x[1])))

    #构建表结构对象：StructType对象
    schema=StructType().add("name",StringType(),nullable=True).\
        add("age",IntegerType(),nullable=False)

    #基于StructType对象构建RDD和DF转换
    df = spark.createDataFrame(rdd, schema=schema)

    df.printSchema()
    df.show()
```

##### 		3.2.3 基于RDD方式（使用toDF方法）

```python
#coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,IntegerType

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    #基于RDD转换DataFrame
    rdd=sc.textFile("../data/input/sql/people.txt").\
        map(lambda x:x.split(",")).\
        map(lambda x:(x[0],int(x[1])))

    #toDF方式构建df
    df = rdd.toDF(["name", "age"])
    df.printSchema()
    df.show()

    #toDF 通过structtype构建
    schema=StructType().add("name",StringType(),nullable=True).\
        add("age",IntegerType(),nullable=False)

    df1=rdd.toDF(schema=schema)
    df1.printSchema()
    df1.show()
```

##### 		3.2.4 基于Pandas的DataFrame

```python
if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    #基于Pandas的DataFrame
    pdf=pd.DataFrame(
        {
            "id":[1,2,3],
            "name":["wang","wu","liu"],
            "age":[11,21,13]
        }
    )

    df=spark.createDataFrame(pdf)
    df.printSchema()
    df.show()

```



#### 3.3 ==标准API==构建DataFrame

![image-20230421223844066](.assets/image-20230421223844066.png)

##### 		3.3.1 读取text数据源（只有一个列）

![image-20230421224447739](.assets/image-20230421224447739.png)

![image-20230421224549073](.assets/image-20230421224549073.png)

##### 		3.3.2 读取json数据（自动识别生成列）

![image-20230421224506332](.assets/image-20230421224506332.png)

![image-20230421224822790](.assets/image-20230421224822790.png)

##### 		3.3.3 读取jcsv数据

![image-20230421224513738](.assets/image-20230421224513738.png)



##### 		3.3.4 读取jparquet数据

![image-20230421224534152](.assets/image-20230421224534152.png)



#### 3.4 DataFrame操作

![image-20230421231050552](.assets/image-20230421231050552.png)

##### ==**DSL风格**==

<u>show()方法</u>

![image-20230421231735108](.assets/image-20230421231735108.png)

<u>printSchema()方法</u>

![image-20230421231815342](.assets/image-20230421231815342.png)

<u>select()方法</u>

![image-20230421231840701](.assets/image-20230421231840701.png)

<u>filter()和where()方法</u>

![image-20230421231916211](.assets/image-20230421231916211.png)

<u>groupBy()方法</u> ==返回对象不是DataFrame==

![image-20230421231953379](.assets/image-20230421231953379.png)



##### ==**SQL风格**==

![image-20230421232702535](.assets/image-20230421232702535.png)

![image-20230421232910718](.assets/image-20230421232910718.png)

==**供sql计算的函数包**==

![image-20230421233245183](.assets/image-20230421233245183.png)



#### 3.5 DataFrame案例

##### 		3.5.1 词频统计

```python
# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    #TODO 1:sql
    rdd=sc.textFile("../data/input/words.txt").\
        flatMap(lambda x:x.split(" ")).\
        map(lambda x:[x])

    df = rdd.toDF(["word"])

    df.createTempView("words")

    spark.sql("select word,COUNT(*) as cnt from words group by word order by cnt desc ").show()


    #TODO 2:dsl
    df = spark.read.format("text").\
        load("../data/input/words.txt")

    # withcolumn
    df2=df.withColumn("value",F.explode(F.split(df['value']," ")))

    df2.groupBy("value").\
        count().\
        withColumnRenamed("value","word").\
        withcolumnRenamed("count","cnt").\
        orderBy("cnt",ascending=False).\
        show()
    
```

##### 		3.5.2 电影评分分析

```python
# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, IntegerType

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    #构建表结构
    schema = StructType().add("user_id", StringType(), nullable=True).add("movie_id", IntegerType(), nullable=True).add(
        "rank", IntegerType(), nullable=True).add("ts", StringType(), nullable=True)

    #1.读取数据集
    df = spark.read.format("csv").\
        option("sep","\t").\
        option("header",False).\
        option("encoding","utf-8").\
        schema(schema=schema).\
        load("../data/input/sql/u.data")

    #TODO 1：用户平均分
    df.groupBy("user_id").\
        avg("rank").\
        withColumn("avg(rank)",F.round("avg(rank)",2)).\
        orderBy("avg(rank)",ascending=False).\
        show()
    df.createTempView("user")
    spark.sql("""
        select user_id,ROUND(AVG(rank),2) as avg_rank from user group by user_id order by avg_rank desc 
    """).show()

    #TODO 2: 电影平均分查询
    df.createTempView("movie")
    spark.sql("""
        select movie_id,ROUND(AVG(rank),2) as avg_rank from movie group by movie_id order by avg_rank desc 
    """).show()

    #TODO 3：查询大于平均分的电影数量
    print("大于平均分的电影数量",df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    #TODO 4：高分电影中次数最多的用户，此人平均的平均分
    user_id=df.where("rank>3").\
        groupBy("user_id").\
        count().\
        orderBy("count",ascending=False).\
        limit(1).\
        first()['user_id']
    df.filter(df['user_id']==user_id).\
        select(F.round(F.avg("rank"),2)).show()

    #TODO 5：查询每个用户平均打分，最低，最高
    df.groupBy("user_id").\
        agg(
        F.round(F.avg("rank"),2).alias("avg_rank"),
        F.min("rank").alias("min_rank"),
        F.max("rank").alias("max_rank")
    ).show()

    #TODO 6: 查询评分超过100次的电影的平均分，排名TOP10
    df.groupBy("movie_id").\
        agg(
        F.count("movie_id").alias("cnt"),
        F.round(F.avg("rank"),2).alias("avg_rank")
    ).where("cnt>100").\
    orderBy("avg_rank",ascending=False).\
    limit(10).\
    show()
```



#### 3.6 SparkSQL Shuffle 分区数目

![image-20230422102844541](.assets/image-20230422102844541.png)



#### 3.7 SparkSQL 数据清洗API

##### 		3.7.1 数据去重（dropDuplicates）

![image-20230422154647569](.assets/image-20230422154647569.png)

##### 		3.7.2 删除缺失值数据（dropna）

![image-20230422154929752](.assets/image-20230422154929752.png)

##### 		3.7.3 填充缺失值数据（fillna）

![image-20230422154940783](.assets/image-20230422154940783.png)



#### 3.8  DataFrame数据写出

![image-20230422155747796](.assets/image-20230422155747796.png)

![image-20230422160100304](.assets/image-20230422160100304.png)



#### 3.9 DataFrame通过JDBC读写数据库（MySQL）

![image-20230422161451077](.assets/image-20230422161451077.png)

![image-20230422161502224](.assets/image-20230422161502224.png)





### 4. SparkSQL函数定义

![image-20230422163201467](.assets/image-20230422163201467.png)



#### 4.1 定义UDF函数

**定义方式**

![image-20230422163232356](.assets/image-20230422163232356.png)

**Interger类型返回值**

```python
# coding:utf8
from pyspark import F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    #构建rdd
    rdd=sc.parallelize([1,2,3,4,5,6,7,8]).map(lambda x:[x])
    df=rdd.toDF(["num"])

    #TODO 1: 方式1 sparksession.udf.redister()
    #udf处理函数
    def num_10(num):
        return num*10

    #参数1 udf名称
    #参数2 处理逻辑函数
    #参数3 udf返回值类型
    udf2 = spark.udf.register("udf1", num_10, IntegerType())

    #sql风格使用
    df.selectExpr("udf1(num)").show()

    #dsl风格
    df.select(udf2(df['num'])).show()


    #TODO 2:方式2 仅dsl风格
    udf3 = F.udf(num_10, IntegerType())
    df.select(udf3(df['num'])).show()


```

**ArrayType(数字\list)**类型返回值

```python
# coding:utf8

from pyspark import F
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType,StringType

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    rdd=sc.parallelize([["hadoop spark flink"], ["hadoop flink java"]])
    df = rdd.toDF(["line"])


    # 注册udf
    def split_line(data):
        return data.split(" ")

    #TODO 1: 构建udf
    udf = spark.udf.register("udf1",split_line,ArrayType(StringType()))

    #dsl风格
    df.select(udf(df['line'])).show()
    #sql风格
    df.createTempView("lines")
    spark.sql("select udf1(line) from lines").show()

    #TODO 2: 仅dsl风格
    udf3=F.udf(split_line,ArrayType(StringType()))
    df.select(udf3(df['line'])).show()
    
```

**字典类型返回值**

```python
# coding:utf8
import string

from pyspark import F
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType,StringType,IntegerType,StructType

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    rdd=sc.parallelize([[1],[2],[3]])
    df = rdd.toDF(["num"])

    #注册udf
    def process(data):
        return {"num":data,"letters":string.ascii_letters[data]}

    udf = spark.udf.register("udf", process,StructType().add("num",IntegerType(),nullable=True).\
                             add("letters",StringType(),nullable=True))

    df.selectExpr("udf(num)").show(truncate=False)
    df.select(udf(df['num'])).show(truncate=False)
    
```



#### 4.2 使用窗口函数

![image-20230423143126109](.assets/image-20230423143126109.png)

**语法：**

![image-20230423143143657](.assets/image-20230423143143657.png)





### 5. SparkSQL运行流程

#### 5.1 SparkRDD执行流程

![image-20230423144243240](.assets/image-20230423144243240.png)



#### 5.2 SparkSQL自动优化

![image-20230423144339197](.assets/image-20230423144339197.png)



#### 5.3 Catalyst优化器

![image-20230423145935662](.assets/image-20230423145935662.png)

![image-20230423145946180](.assets/image-20230423145946180.png)

![image-20230423145954914](.assets/image-20230423145954914.png)

![image-20230423150012345](.assets/image-20230423150012345.png)



#### 5.4 SparkSQL执行流程

![image-20230423150029909](.assets/image-20230423150029909.png)





### 6. SparkSQL整合Hive

#### 6.1 原理

![image-20230423151739581](.assets/image-20230423151739581.png)



#### 6.2 配置

![image-20230423152113964](.assets/image-20230423152113964.png)

![image-20230423152123143](.assets/image-20230423152123143.png)

![image-20230423152135701](.assets/image-20230423152135701.png)



#### 6.3 在代码中集成

![image-20230423154259176](.assets/image-20230423154259176.png)





### 7. 分布式SQL引擎配置

#### 7.1 概念

![image-20230423215622103](.assets/image-20230423215622103.png)

![image-20230423220454198](.assets/image-20230423220454198.png)

```python
#在spark包下 spark提供的ThriftServer 底层翻译成rdd运行
sbin/start-thriftserver.sh --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.server2.thrift.bind.host=node1 --master local[*]
```



#### 7.2 客户端工具

![image-20230423222549542](.assets/image-20230423222549542.png)



#### 7.3 代码JDBC连接

```python
# coding:utf8

from pyhive import hive
from pyspark.sql import SparkSession

if __name__ == '__main__':
    #获取hive
    conn=hive.Connection(host="node1",port=10000,username="root")

    #游标对象
    cursor = conn.cursor()

    #执行sql
    cursor.execute("select * from student")

    #通过fetchall
    result = cursor.fetchall()

    print(result)

```





## 四、Spark新特性

### 1. SparkShuffle

![image-20230424162532249](.assets/image-20230424162532249.png)



#### 1.1 HashShuffleManager

![image-20230424162644495](.assets/image-20230424162644495.png)



#### 1.2 SortShuffleManager

![image-20230424162755343](.assets/image-20230424162755343.png)

![image-20230424163052152](.assets/image-20230424163052152.png)



### 2. Spark3.0

![image-20230424163606697](.assets/image-20230424163606697.png)





## 五、 Spark概念总结



### 1.  部署模式

![image-20230424164626131](.assets/image-20230424164626131.png)



### 2. RDD

#### 2.1 概念 特点

<img src=".assets/image-20230424165041974.png" alt="image-20230424165041974" style="zoom:200%;" />



#### 2.2 算子

![image-20230424165139717](.assets/image-20230424165139717.png)



#### 2.3 缓存

![image-20230424165204432](.assets/image-20230424165204432.png)



#### 2.4 CheckPoint

![image-20230424165226503](.assets/image-20230424165226503.png)



#### 2.5 Spark执行流程

![image-20230424165239749](.assets/image-20230424165239749.png)



#### 2.6  DAG Scheduler 和 Task Scheduler

![image-20230424165300756](.assets/image-20230424165300756.png)



#### 2.7 内存迭代

![image-20230424165318711](.assets/image-20230424165318711.png)





### 3. DataFrame

![image-20230424165358082](.assets/image-20230424165358082.png)





### 4. SparkSQL执行流程

![image-20230424165434020](.assets/image-20230424165434020.png)





### 5. 层次关系

#### 5.1 Spark环境

![image-20230424165743292](.assets/image-20230424165743292.png)



#### 5.2 宽窄依赖

![image-20230424165800577](.assets/image-20230424165800577.png)



<img src=".assets/image-20230424165613032.png" alt="image-20230424165613032" style="zoom:200%;" />