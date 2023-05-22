# 							Flink

## 一、 Flink简介

### 1. Flink是什么

 ![image-20230425093144355](../.assets/image-20230425093144355-16837262623921.png)



### 2. 处理流程

![image-20230425093516922](../.assets/image-20230425093516922.png)





### 3. 数据处理框架

![image-20230425095709773](../.assets/image-20230425095709773.png)





### 4. 流处理应用

![image-20230425095822104](../.assets/image-20230425095822104.png)





### 5. 分层API

![image-20230425100200286](../.assets/image-20230425100200286.png)





### 6. Flink和Spark区别

![image-20230425100330045](../.assets/image-20230425100330045.png)

![image-20230425100408371](../.assets/image-20230425100408371.png)







## 二、 Flink快速上手

### 1. 环境准备

创建maven项目，导入pom文件

```xml
<properties>
        <flink.version>1.13.0</flink.version>
        <java.version>1.8</java.version>
        <scala.binary.version>2.12</scala.binary.version>
        <slf4j.version>1.7.30</slf4j.version>
    </properties>
    <dependencies>
        <!-- 引入 Flink 相关依赖-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <!-- 引入日志管理相关依赖-->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-to-slf4j</artifactId>
            <version>2.14.0</version>
        </dependency>
    </dependencies>
```





### 2. 批处理word_count

```java

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. 从文件读取数据
        DataSource<String> lineDataource = env.readTextFile("input/words.txt");

        //3. 将每行数据进行分词转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordOneTuple = lineDataource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            //将一行文本进行分词
            String[] words = line.split(" ");
            //将每个单词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4.按照每个word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordOneGroup = wordOneTuple.groupBy(0);

        //5. 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordOneTuple.sum(1);

        //6. 打印结果
        sum.print();

    }
}
```





### 3. 流处理word_count

1. 有界流处理

```java
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        //3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordOneKeyStream = wordOneTuple.keyBy(data->data.f0);

        //5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordOneKeyStream.sum(1);

        //6.打印
        sum.print();

        //7.启动执行
        env.execute();
    }
}
```

2. 无界流处理

```java
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从参数提取主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        //2. 读取文件流
        DataStreamSource<String> lineDataStream = env.socketTextStream(hostname, port);

        //3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordOneKeyStream = wordOneTuple.keyBy(data->data.f0);

        //5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordOneKeyStream.sum(1);

        //6.打印
        sum.print();

        //7.启动执行
        env.execute();
    }
}
```

![image-20230425152211497](../.assets/image-20230425152211497.png)







## 三、 Flink部署

### 1.  启动集群

![image-20230425152800335](../.assets/image-20230425152800335.png)

```
#启动集群
bin/start-cluster.sh 

#web ui网址
http://node1:8081
```





### 2. 向集群提交作业











## 四、 Flink 运行时架构

### 1. 系统架构

#### 		1.1 整体构成

Flink 的运行时架构中，最重要的就是两大组件：==作业管理器（JobManger）==和==任务管理器 （TaskManager）==。对于一个提交执行的作业，JobManager 是真正意义上的“管理者”（Master）， 负责管理调度，所以在不考虑高可用的情况下只能有一个；而 TaskManager 是“工作者” （Worker、Slave），负责执行任务处理数据，所以可以有一个或多个。

![image-20230510222525556](../.assets/image-20230510222525556.png)

TaskManager 启动之后，JobManager 会与它建立连接，并将作业图（JobGraph）转换成可 执行的“执行图”（ExecutionGraph）分发给可用的 TaskManager，然后就由 TaskManager 具体 执行任务。接下来，我们就具体介绍一下 JobManger 和 TaskManager 在整个过程中扮演的角色。



#### 		1.2 作业管理器

JobManager 是一个 Flink 集群中任务管理和调度的核心，是控制应用执行的主进程。也就 是说，每个应用都应该被唯一的 JobManager 所控制执行。

1. JobMaster JobMaster 是 JobManager 中最核心的组件，负责处理单独的作业（Job）。所以 JobMaster 和具体的 Job 是一一对应的，多个 Job 可以同时运行在一个 Flink 集群中, 每个 Job 都有一个 自己的 JobMaster。需要注意在早期版本的 Flink 中，没有 JobMaster 的概念；而 JobManager 的概念范围较小，实际指的就是现在所说的 JobMaster。 在作业提交时，JobMaster 会先接收到要执行的应用。这里所说“应用”一般是客户端提 交来的，包括：Jar 包，数据流图（dataflow graph），和作业图（JobGraph）。 JobMaster 会把 JobGraph 转换成一个物理层面的数据流图，这个图被叫作“执行图” （ExecutionGraph），它包含了所有可以并发执行的任务。 JobMaster 会向资源管理器 （ResourceManager）发出请求，申请执行任务必要的资源。一旦它获取到了足够的资源，就会 将执行图分发到真正运行它们的 TaskManager 上。 而在运行过程中，JobMaster 会负责所有需要中央协调的操作，比如说检查点（checkpoints） 的协调。 
2. 资源管理器（ResourceManager） ResourceManager 主要负责资源的分配和管理，在 Flink 集群中只有一个。所谓“资源”， 主要是指 TaskManager 的任务槽（task slots）。任务槽就是 Flink 集群中的资源调配单元，包含 了机器用来执行计算的一组 CPU 和内存资源。每一个任务（Task）都需要分配到一个 slot 上 执行。 这里注意要把 Flink 内置的 ResourceManager 和其他资源管理平台（比如 YARN）的 ResourceManager 区分开。 Flink 的 ResourceManager，针对不同的环境和资源管理平台（比如 Standalone 部署，或者 YARN），有不同的具体实现。在 Standalone 部署时，因为 TaskManager 是单独启动的（没有 Per-Job 模式），所以 ResourceManager 只能分发可用 TaskManager 的任务槽，不能单独启动新 TaskManager。 而在有资源管理平台时，就不受此限制。当新的作业申请资源时，ResourceManager 会将 有空闲槽位的 TaskManager 分配给 JobMaster。如果 ResourceManager 没有足够的任务槽，它 还可以向资源提供平台发起会话，请求提供启动 TaskManager 进程的容器。另外， ResourceManager 还负责停掉空闲的 TaskManager，释放计算资源。 
3. 分发器（Dispatcher） Dispatcher 主要负责提供一个 REST 接口，用来提交应用，并且负责为每一个新提交的作 业启动一个新的 JobMaster 组件。Dispatcher 也会启动一个 Web UI，用来方便地展示和监控作 业执行的信息。



#### 		1.3 任务管理器

TaskManager 是 Flink 中的工作进程，数据流的具体计算就是它来做的，所以也被称为 “Worker”。Flink 集群中必须至少有一个 TaskManager；当然由于分布式计算的考虑，通常会 有多个 TaskManager 运行，每一个 TaskManager 都包含了一定数量的任务槽（task slots）。Slot 是资源调度的最小单位，slot 的数量限制了 TaskManager 能够并行处理的任务数量。 

启动之后，TaskManager 会向资源管理器注册它的 slots；收到资源管理器的指令后， TaskManager 就会将一个或者多个槽位提供给 JobMaster 调用，JobMaster 就可以分配任务来执 行了。 

在执行过程中，TaskManager 可以缓冲数据，还可以跟其他运行同一应用的 TaskManager 交换数据。





### 2. 重要概念

#### 		2.1 数据流图

Flink 是流式计算框架。它的程序结构，其实就是定义了一连串的处理操作，每一个数据 输入之后都会依次调用每一步计算。在 Flink 代码中，我们定义的每一个处理转换操作都叫作 “算子”（Operator），所以我们的程序可以看作是一串算子构成的管道，数据则像水流一样有序 地流过。比如在之前的 WordCount 代码中，基于执行环境调用的 socketTextStream()方法，就 是一个读取文本流的算子；而后面的 flatMap()方法，则是将字符串数据进行分词、转换成二 元组的算子。 所有的 Flink 程序都可以归纳为由三部分构成：Source、Transformation 和 Sink。 

⚫ Source 表示“源算子”，负责读取数据源。 

⚫ Transformation 表示“转换算子”，利用各种算子进行处理加工。 

⚫ Sink 表示“下沉算子”，负责数据的输出。

![image-20230510223100943](../.assets/image-20230510223100943.png)



#### 		2.2 并行度

对于 Spark 而言，是把根据程序生成的 DAG 划分阶段（stage）、进而分配任务的。而对于 Flink 这样的流 式引擎，其实没有划分 stage 的必要。

![image-20230510223200125](../.assets/image-20230510223200125.png)

**==并行度设置==**

```
1. 在代码中设置（只针对当前算子有效）
stream.map(word -> Tuple2.of(word, 1L)).setParallelism(2);
env.setParallelism(2);

2. 提交应用时设置
bin/flink run –p 2 –c com.atguigu.wc.StreamWordCount 
./FlinkTutorial-1.0-SNAPSHOT.jar

3. 配置文件中设置
parallelism.default: 2
```



#### 		2.3 算子链







## 五、 DataStream API

DataStream（数据流）本身是 Flink 中一个用来表示数据集合的类（Class），我们编写的 Flink 代码其实就是基于这种数据类型的处理，所以这套核心 API 就以 DataStream 命名。对于 批处理和流处理，我们都可以用这同一套 API 来实现。

⚫ 获取执行环境（execution environment） 

⚫ 读取数据源（source） 

⚫ 定义基于数据的转换操作（transformations） 

⚫ 定义计算结果的输出位置（sink） 

⚫ 触发程序执行（execute）



![image-20230510224559435](../.assets/image-20230510224559435.png)





### 1. 执行环境

#### 	1.1 创建执行环境

##### 			1.1.1  getExecutionEnvironment 

最简单的方式，就是直接调用 getExecutionEnvironment 方法。它会根据当前运行的上下文 直接得到正确的结果：如果程序是独立运行的，就返回一个本地执行环境；如果是创建了 jar 包，然后从命令行调用它并提交到集群执行，那么就返回集群的执行环境。也就是说，这个方 法会根据当前运行的方式，自行决定该返回什么样的运行环境。

```java
StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
```

##### 			1.1.2 createLocalEnvironment

这个方法返回一个本地执行环境。可以在调用时传入一个参数，指定默认的并行度；如果 不传入，则默认并行度就是本地的 CPU 核心数。

```java
StreamExecutionEnvironment localEnv = 
StreamExecutionEnvironment.createLocalEnvironment();
```

##### 			1.1.3 createRemoteEnvironment

这个方法返回集群执行环境。需要在调用时指定 JobManager 的主机名和端口号，并指定 要在集群中运行的 Jar 包。

```java
StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment
 .createRemoteEnvironment(
 "host", // JobManager 主机名
 1234, // JobManager 进程端口号
 "path/to/jarFile.jar" // 提交给 JobManager 的 JAR 包
); 
```



#### 	1.2 执行环境

```java
// 批处理环境
ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
// 流处理环境
StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
```



#### 	1.3 程序执行

```java
env.execute();
```





### 2. 源算子

Flink 代码中通用的添加 source 的方式，是调用执行环境的 addSource()方法：

```java
//方法传入一个对象参数，需要实现 SourceFunction 接口；返回 DataStreamSource。这里的DataStreamSource 类继承自 SingleOutputStreamOperator 类，又进一步继承自 DataStream。所以很明显，读取数据的 source 操作是一个算子，得到的是一个数据流（DataStream）
DataStream<String> stream = env.addSource(...);
```



#### 	2.1 从集合中读取数据

```java
public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
 ArrayList<Event> clicks = new ArrayList<>();
 clicks.add(new Event("Mary","./home",1000L));
 clicks.add(new Event("Bob","./cart",2000L));
 DataStream<Event> stream = env.fromCollection(clicks);
stream.print();
 env.execute();
}

//我们也可以不构建集合，直接将元素列举出来，调用 fromElements 方法进行读取数据：
DataStreamSource<Event> stream2 = env.fromElements(
 new Event("Mary", "./home", 1000L),
 new Event("Bob", "./cart", 2000L)
);
```



#### 	2.2 从文件读取数据

```java
DataStream<String> stream = env.readTextFile("clicks.csv");
```

⚫ 参数可以是目录，也可以是文件； 

⚫ 路径可以是相对路径，也可以是绝对路径； 

⚫ 相对路径是从系统属性 user.dir 获取路径: idea 下是 project 的根目录, standalone 模式 下是集群节点根目录； 

⚫ 也可以从 hdfs 目录下读取, 使用路径 hdfs://..., 由于 Flink 没有提供 hadoop 相关依赖, 

需要 pom 中添加相关依赖:

```xml
<dependency>
 <groupId>org.apache.hadoop</groupId>
 <artifactId>hadoop-client</artifactId>
 <version>2.7.5</version>
 <scope>provided</scope>
</dependency>
```



#### 	2.3 从Socket读取数据（用于测试）

```java
DataStream<String> stream = env.socketTextStream("localhost", 7777);
```



#### 	2.4 从Kafka读取数据

```xml
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
 <version>${flink.version}</version>
</dependency>
```



调用 env.addSource()，传入 FlinkKafkaConsumer 的对象实例

```java
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;
public class SourceKafkaTest {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
 Properties properties = new Properties();
 properties.setProperty("bootstrap.servers", "hadoop102:9092");
 properties.setProperty("group.id", "consumer-group");
 properties.setProperty("key.deserializer", 
"org.apache.kafka.common.serialization.StringDeserializer");
 properties.setProperty("value.deserializer", 
"org.apache.kafka.common.serialization.StringDeserializer");
 properties.setProperty("auto.offset.reset", "latest");
 DataStreamSource<String> stream = env.addSource(new 
FlinkKafkaConsumer<String>(
 "clicks",
 new SimpleStringSchema(),
 properties
 ));
77
 stream.print("Kafka");
 env.execute();
 }
}
```

创建 FlinkKafkaConsumer 时需要传入三个参数： 

⚫ 第一个参数 topic，定义了从哪些主题中读取数据。可以是一个 topic，也可以是 topic 列表，还可以是匹配所有想要读取的 topic 的正则表达式。当从多个 topic 中读取数据 时，Kafka 连接器将会处理所有 topic 的分区，将这些分区的数据放到一条流中去。 

⚫ 第二个参数是一个 DeserializationSchema 或者 KeyedDeserializationSchema。Kafka 消 息被存储为原始的字节数据，所以需要反序列化成 Java 或者 Scala 对象。上面代码中 使用的 SimpleStringSchema，是一个内置的 DeserializationSchema，它只是将字节数 组简单地反序列化成字符串。DeserializationSchema 和 KeyedDeserializationSchema 是 公共接口，所以我们也可以自定义反序列化逻辑。 

⚫ 第三个参数是一个 Properties 对象，设置了 Kafka 客户端的一些属性。



#### 	2.5 Flink支持的数据类型

（1）基本类型 所有 Java 基本类型及其包装类，再加上 Void、String、Date、BigDecimal 和 BigInteger。  

（2）数组类型 包括基本类型数组（PRIMITIVE_ARRAY）和对象数组(OBJECT_ARRAY) 

（3）复合数据类型 

​		⚫ Java 元组类型（TUPLE）：这是 Flink 内置的元组类型，是 Java API 的一部分。最多 25 个字段，也就是从 Tuple0~Tuple25，不支持空字段 

​		⚫ Scala 样例类及 Scala 元组：不支持空字段 

​		⚫ 行类型（ROW）：可以认为是具有任意个字段的元组,并支持空字段 

​		⚫ POJO：Flink 自定义的类似于 Java bean 模式的类 

（4）辅助类型 Option、Either、List、Map 等 

（5）泛型类型（GENERIC） Flink 支持所有的 Java 类和 Scala 类。不过如果没有按照上面 POJO 类型的要求来定义， 就会被 Flink 当作泛型类来处理。Flink 会把泛型类型当作黑盒，无法获取它们内部的属性；它 们也不是由 Flink 本身序列化的，而是由 Kryo 序列化的。





### 3. 转换算子

#### 	3.1 基本转换算子

##### 		3.1.1  map映射

基于 DataStrema 调用 map()方法就可以进行转换处理。方法需要传入的参数是 接口 MapFunction 的实现；返回值类型还是 DataStream，不过泛型（流中的元素类型）可能改变。

![image-20230513161410309](../.assets/image-20230513161410309.png)

```java

public class Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./port", 2000L),
                new Event("Mary", "./arr", 3000L)
        );

        //自定义函数进行转换算子
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());

        //使用匿名函数实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        //传入lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);

        result1.print();
        result2.print();
        result3.print();
        env.execute();

    }

    //自定义mapfunction
    public static class MyMapper implements MapFunction<Event,String>{

        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}

```

**<u>map 是将一个 DataStream 转换成另一个 DataStream</u>**



##### 		3.1.2  filter过滤

对数据流执行一个过滤，通过一个布尔条件表达式设置过滤 条件，对于每一个流内元素进行判断，若为 true 则元素正常输出，若为 false 则元素被过滤掉。

进行 filter 转换之后的新数据流的数据类型与原数据流是相同的。filter 转换需要传入的参 数需要实现 FilterFunction 接口，而 FilterFunction 内要实现 filter()方法，就相当于一个返回布 尔类型的条件表达式。

![image-20230513161423417](../.assets/image-20230513161423417.png)

```java

public class filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./port", 2000L),
                new Event("Alice", "./arr", 3000L)
        );

        //传入一个实现了filterfunction的类对象
        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyFilter());

        //使用匿名函数实现filterfunction
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Alice");
            }
        });

        //使用lambda
        SingleOutputStreamOperator<Event> result3 = stream.filter(data -> data.user.equals("Alice"));


        result1.print();
        result2.print();
        result3.print();
        env.execute();
    }
    //实自定义的filterfunction
    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}
```



##### 		3.1.3 flatMap扁平映射 map+filter

主要是将数据流中的整体（一般是集合类型）拆分成一个 一个的个体使用。消费一个元素，可以产生 0 到多个元素。flatMap 可以认为是“扁平化”（flatten） 和“映射”（map）两步操作的结合，也就是先按照某种规则对数据进行打散拆分，再对拆分 后的元素做转换处理。

flatMap 并没有直接定义返回值类型，而是通过一个“收集器”（Collector）来 指定输出。希望输出结果时，只要调用收集器的.collect()方法就可以。

![image-20230513161435465](../.assets/image-20230513161435465.png)

```java
public class TransFlatmapTest {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
 DataStreamSource<Event> stream = env.fromElements(
 new Event("Mary", "./home", 1000L),
 new Event("Bob", "./cart", 2000L)
 );
 stream.flatMap(new MyFlatMap()).print();
 env.execute();
 }
 public static class MyFlatMap implements FlatMapFunction<Event, String> {
 @Override
 public void flatMap(Event value, Collector<String> out) throws Exception 
{
 if (value.user.equals("Mary")) {
 out.collect(value.user);
 } else if (value.user.equals("Bob")) {
 out.collect(value.user);
 out.collect(value.url);
 }
 }
 }
} 
```





#### 	3.2 聚合算子

##### 		3.2.1 keyBy按键分区

对于 Flink 而言，DataStream 是没有直接进行聚合的 API 的。因为我们对海量数据做聚合 肯定要进行分区并行处理，这样才能提高效率。所以在 Flink 中，==要做聚合，需要先进行分区==； 这个操作就是通过 keyBy 来完成的。

==keyBy 是聚合前必须要用到的一个算子==。keyBy 通过指定键（key），可以==将一条流从逻辑 上划分成不同的分区==（partitions）。这里所说的分区，其实就是并行处理的子任务，也就对应 着任务槽（task slot）。

![image-20230514151147737](../.assets/image-20230514151147737.png)

keyBy()方法需要==传入一个参数==，这个参数指定了一个或一组 key。有很多不同的方法来指 定 key：比如对于 Tuple 数据类型，可以指定字段的位置或者多个位置的组合；对于 POJO 类 型，可以指定字段的名称（String）；另外，还可以传入 Lambda 表达式或者实现一个键选择器 （KeySelector），用于说明从数据中提取 key 的逻辑。

```java
public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./port", 2000L),
                new Event("Mary", "./arr", 3000L)
        );

        //按键分组之后进行聚合，提取当前用户最后一次访问
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp").print("max: ");

        stream.keyBy(data->data.user).maxBy("timestamp").print("maxBy: ");

        env.execute();
    }
```

keyBy 得到的结果将不再是 DataStream，而是会将 DataStream 转换为 ==KeyedStream==。KeyedStream 是一个非常重要的数据结构，只有**基于它才可以做后续的聚合操 作**（比如 sum，reduce）；而且它可以将当前算子任务的状态（state）也按照 key 进行划分、限 定为仅对当前 key 有效。



##### 3.2.2 简单聚合

⚫ sum()：在输入流上，对指定的字段做叠加求和的操作。 

⚫ min()：在输入流上，对指定的字段求最小值。 

⚫ max()：在输入流上，对指定的字段求最大值。 

⚫ minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包 含字段最小值的整条数据。 

⚫ maxBy()：与 max()类似，在输入流上针对指定字段求最大值。两者区别与 min()/minBy()完全一致。

简单聚合算子返回的，同样是一个 SingleOutputStreamOperator，也就是从 KeyedStream 又 转换成了常规的 DataStream。所以可以这样理解：**keyBy 和聚合是成对出现的，先分区、后聚 合，得到的依然是一个 DataStream。**而且经过简单聚合之后的数据流，元素的数据类型保持不 变。



##### 3.2.3 reduce归约聚合

reduce 算子就是一个一般化的聚合 统计操作。它可以对已有的 数据进行归约处理，把每一个新输入的数据和当前已经归约出来的值，再做一个聚合计算。reduce 操作也会将 KeyedStream 转换为 DataStream。它不会改变流的元 素数据类型，所以输出类型和输入类型是一样的。

调用 KeyedStream 的 reduce 方法时，需要传入一个参数，实现 **ReduceFunction** 接口

接口源码：

```java
public interface ReduceFunction<T> extends Function, Serializable {
T reduce(T value1, T value2) throws Exception;
}
```



```java
public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./port", 2000L),
                new Event("Mary", "./arr", 3000L)
        );

        //点击次数最多的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        }).keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        //选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickByUser.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        });

        result.print();
        env.execute();
    }
```





#### 3.3 用户自定义函数（UDF）

Flink 的 DataStream API 编程风格其实是一致的：基本上都 是==基于 DataStream 调用==一个方法，表示要做一个转换操作；方法需要传入一个参数，这个参 数都是需要实现一个接口。全部都以算子操作名称 + Function 命名。我们不仅可以通过自定义函数类或者匿名类来 实现接口，也可以直接传入 Lambda 表达式。

##### 3.3.1  函数类

对于大部分操作而言，都需要==传入一个用户自定义函数==（UDF），实现相关操作的接口， 来完成处理逻辑的定义。Flink 暴露了所有 UDF 函数的接口，具体实现方式为==接口或者抽象类==， 例如 MapFunction、FilterFunction、ReduceFunction 等。 所以最简单直接的方式，就是自定义一个函数类，实现对应的接口。

```java
public class TransFunctionUDFTest {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
93
 DataStreamSource<Event> clicks = env.fromElements(
 new Event("Mary", "./home", 1000L),
 new Event("Bob", "./cart", 2000L)
 );
 DataStream<Event> stream = clicks.filter(new FlinkFilter());
 stream.print();
env.execute();
 }
 public static class FlinkFilter implements FilterFunction<Event> {
 @Override
 public boolean filter(Event value) throws Exception {
 return value.url.contains("home");
 }
 }
}

```



还可以通过匿名类来实现 FilterFunction 接口：

```java
DataStream<String> stream = clicks.filter(new FilterFunction<Event>() {
 @Override
 public boolean filter(Event value) throws Exception {
 return value.url.contains("home");
 }
});
```

为了类可以更加通用，我们还可以将用于过滤的关键字"home"抽象出来作为类的属性， 调用构造方法时传进去。

```java
DataStream<Event> stream = clicks.filter(new KeyWordFilter("home"));
public static class KeyWordFilter implements FilterFunction<Event> {
 private String keyWord;
 KeyWordFilter(String keyWord) { this.keyWord = keyWord; }
 @Override
 public boolean filter(Event value) throws Exception {
 return value.url.contains(this.keyWord);
 }
}
```



##### 3.3.2 匿名函数

Flink 的所有算子都可以使用 Lambda 表达式的方式来进行编码，但是，**当 Lambda 表 达式使用 Java 的泛型时，我们需要显式的声明类型信息**。

```java
public class TransFunctionLambdaTest {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
 DataStreamSource<Event> clicks = env.fromElements(
 new Event("Mary", "./home", 1000L),
 new Event("Bob", "./cart", 2000L)
 );
 //map 函数使用 Lambda 表达式，返回简单类型，不需要进行类型声明
 DataStream<String> stream1 = clicks.map(event -> event.url);
 stream1.print();
 
 env.execute();
 }
}

```



当使用 map() 函数返回 Flink 自定义的元组类型时也会发生类似的问题。下例中的函数签 名 Tuple2 map(Event value) 被类型擦除为 Tuple2 map(Event value)。

```java
public class ReturnTypeResolve {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
 DataStreamSource<Event> clicks = env.fromElements(
 new Event("Mary", "./home", 1000L),
 new Event("Bob", "./cart", 2000L)
 );
 // 想要转换成二元组类型，需要进行以下处理
96
 // 1) 使用显式的 ".returns(...)"
 DataStream<Tuple2<String, Long>> stream3 = clicks
 .map( event -> Tuple2.of(event.user, 1L) )
 .returns(Types.TUPLE(Types.STRING, Types.LONG));
 stream3.print();
 // 2) 使用类来替代 Lambda 表达式
 clicks.map(new MyTuple2Mapper())
 .print();
 // 3) 使用匿名类来代替 Lambda 表达式
 clicks.map(new MapFunction<Event, Tuple2<String, Long>>() {
 @Override
 public Tuple2<String, Long> map(Event value) throws Exception {
 return Tuple2.of(value.user, 1L);
 }
 }).print();
 env.execute();
 }
 // 自定义 MapFunction 的实现类
 public static class MyTuple2Mapper implements MapFunction<Event, Tuple2<String, 
Long>>{
 @Override
 public Tuple2<String, Long> map(Event value) throws Exception {
 return Tuple2.of(value.user, 1L);
 }
 }
}

```



##### 3.3.3 富函数类

“富函数类”也是 **DataStream API 提供的一个函数类的接口**，所有的 Flink 函数类都有其 Rich 版本。富函数类一般是以抽象类的形式出现的。例如：RichMapFunction、RichFilterFunction、 RichReduceFunction 等。

**与常规函数类的不 同主要在于，富函数类可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现 更复杂的功能。**

Rich Function 有生命周期的概念。典型的生命周期方法有： 

⚫ open()方法，是 Rich Function 的初始化方法，也就是会开启一个算子的生命周期。当 一个算子的实际工作方法例如 map()或者 filter()方法被调用之前，open()会首先被调 用。所以像文件 IO 的创建，数据库连接的创建，配置文件的读取等等这样一次性的 工作，都适合在 open()方法中完成。

⚫ close()方法，是生命周期中的最后一个调用的方法，类似于解构方法。一般用来做一 些清理工作。

```java
public class RichFunctionTest {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(2);
 DataStreamSource<Event> clicks = env.fromElements(
 new Event("Mary", "./home", 1000L),
 new Event("Bob", "./cart", 2000L),
 new Event("Alice", "./prod?id=1", 5 * 1000L),
 new Event("Cary", "./home", 60 * 1000L)
 );
 // 将点击事件转换成长整型的时间戳输出
 clicks.map(new RichMapFunction<Event, Long>() {
 @Override
 public void open(Configuration parameters) throws Exception {
 super.open(parameters);
 System.out.println(" 索 引 为 " + 
getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
 }
 @Override
 public Long map(Event value) throws Exception {
 return value.timestamp;
 }
 @Override
 public void close() throws Exception {
 super.close();
 System.out.println(" 索 引 为 " + 
getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
 }
 })
 .print();
 env.execute();
 }
}

```



如果我们希望连接到==**外部数据库进行读写操作**==，那么将连 接操作放在 map()中显然不是个好选择——因为每来一条数据就会重新连接一次数据库；所以 **我们可以在 open()中建立连接，在 map()中读写数据，而在 close()中关闭连接**。

```java
public class MyFlatMap extends RichFlatMapFunction<IN, OUT>> {
 @Override
 public void open(Configuration configuration) {
 // 做一些初始化工作
 // 例如建立一个和 MySQL 的连接
 }
 @Override
 public void flatMap(IN in, Collector<OUT out) {
 // 对数据库进行读写
 }
 @Override
 public void close() {
 // 清理工作，关闭和 MySQL 数据库的连接。
 }
}

```





#### 3.4 物理分区

##### 3.4.1 随机分区（洗牌）

最简单的重分区方式就是直接“洗牌”。通过调用==**DataStream 的.shuffle()方法**==，将数据随 机地分配到下游算子的并行任务中去。

经过随机分区之后，得到的依然是一个 DataStream。

==**随机分配**==

![image-20230515203112865](../.assets/image-20230515203112865.png)

```java
public class ShuffleTest {
 public static void main(String[] args) throws Exception {
 // 创建执行环境
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
// 读取数据源，并行度为 1
 DataStreamSource<Event> stream = env.addSource(new ClickSource());
// 经洗牌后打印输出，并行度为 4
 stream.shuffle().print("shuffle").setParallelism(4);
 env.execute();
 }
}
```



##### 3.4.2 轮询分区（发牌）

简单来说就是“发牌”，按照先后顺序将数据做依次分 发，通过调用 DataStream 的.rebalance()方法，就可以实现轮询重分区。rebalance 使用的是 Round-Robin 负载均衡算法，可以将输入流数据平均分配到下游的并行任务中去。==**均匀分配**==

![image-20230515205231935](../.assets/image-20230515205231935.png)

```java
public class RebalanceTest {
 public static void main(String[] args) throws Exception {
 // 创建执行环境
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
// 读取数据源，并行度为 1
 DataStreamSource<Event> stream = env.addSource(new ClickSource());
// 经轮询重分区后打印输出，并行度为 4
 stream.rebalance().print("rebalance").setParallelism(4);
 env.execute();
 }
}
```



##### 3.4.3 重缩放分区

当调用 ==rescale()==方法时，其实底层也是使用 Round-Robin 算法进行轮询，但是只会将数据轮询发送到下游并行任务的一部分中，也就 是说，“发牌人”如果有多个，那么 rebalance 的方式是每个发牌人都面向所有人发牌；<u>而 rescale 的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌。</u>

![image-20230515205545020](../.assets/image-20230515205545020.png)

从底层实现上看，rebalance 和 rescale 的根本区别在于任务之间的连接机制不同。rebalance 将会针对所有上游任务（发送数据方）和所有下游任务（接收数据方）之间建立通信通道，这 是一个笛卡尔积的关系；<u>而 rescale 仅仅针对每一个任务和下游对应的部分任务之间建立通信 通道，节省了很多资源。</u>

```java
public class RescaleTest {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
// 这里使用了并行数据源的富函数版本
// 这样可以调用 getRuntimeContext 方法来获取运行时上下文的一些信息
 env
 .addSource(new RichParallelSourceFunction<Integer>() {
 @Override
 public void run(SourceContext<Integer> sourceContext) throws 
Exception {
 for (int i = 0; i < 8; i++) {
 // 将奇数发送到索引为 1 的并行子任务
 // 将偶数发送到索引为 0 的并行子任务
 if ((i + 1) % 2 == 
getRuntimeContext().getIndexOfThisSubtask()) {
 sourceContext.collect(i + 1);
 }
 }
 }
 @Override
 public void cancel() {
 }
 })
 .setParallelism(2)
 .rescale()
 .print().setParallelism(4);
 env.execute();
 }
}

```



##### 3.4.4 广播

经过广播之后，数据会在不同的分区都保留一 份，可能进行重复处理。可以通过调用 ==**DataStream 的 broadcast()方法**==，将输入数据复制并发送 到下游算子的所有并行任务中去。

```java
public class BroadcastTest {
 public static void main(String[] args) throws Exception {
 // 创建执行环境
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
// 读取数据源，并行度为 1
 DataStreamSource<Event> stream = env.addSource(new ClickSource());
// 经广播后打印输出，并行度为 4
 stream. broadcast().print("broadcast").setParallelism(4);
 env.execute();
 }
}
```



##### 3.4.5 全局分区

全局分区也是一种特殊的分区方式。这种做法非常极端，通过**==调用.global()方法==**，会将所有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行 度变成了 1，所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。



##### 3.4.6 自定义分区

当 Flink 提 供 的 所 有 分 区 策 略 都 不 能 满 足 用 户 的 需 求 时 ， 我 们 可 以 通 过 使 用 **==partitionCustom()方法==**来自定义分区策略。

在调用时，方法需要传入两个参数，第一个是**自定义分区器（Partitioner）对象**，第二个 是**应用分区器的字段**，它的指定方式与 keyBy 指定 key 基本一样：可以通过字段名称指定， 也可以通过字段位置索引来指定，还可以实现一个 KeySelector。

```java
public class CustomPartitionTest {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
// 将自然数按照奇偶分区
 env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
 .partitionCustom(new Partitioner<Integer>() {
 @Override
 public int partition(Integer key, int numPartitions) {
 return key % 2;
 }
 }, new KeySelector<Integer, Integer>() {
 @Override
 public Integer getKey(Integer value) throws Exception {
 return value;
 }
 })
 .print().setParallelism(2);
 env.execute();
 }
}

```





### 4. 输出算子

#### 4.1 连接到外部系统







#### 4.2 输出到文件





#### 4.3 输出到Kafka







#### 4.4 输出到Redis





#### 4.5 输出到Elasticsearch





#### 4.6 输出到Mysql





#### 4.7 自定义Sink输出







## 六、 









