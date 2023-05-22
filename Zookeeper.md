# Zookeeper

## 一、 Zookeeper入门

### 1. 概述

![image-20230516141750807](.assets/image-20230516141750807.png)





### 2. 特点

![image-20230516142137463](.assets/image-20230516142137463.png)





### 3. 数据结构

![image-20230516142418615](.assets/image-20230516142418615.png)





### 4. 应用场景

#### 4.1 统一命名服务

![image-20230516142542476](.assets/image-20230516142542476.png)



#### 4.2 统一配置管理

![image-20230516142602875](.assets/image-20230516142602875.png)



#### 4.3 统一集群管理

![image-20230516142629249](.assets/image-20230516142629249.png)



#### 4.4 服务器动态上下线

![image-20230516142725447](.assets/image-20230516142725447.png)



#### 4.5 软负载均衡

![image-20230516142744211](.assets/image-20230516142744211.png)







## 二、 ==启动Zookeeper==

```shell
[root@node1 zookeeper]# bin/zkServer.sh start
```







## 三、 Zookeeper集群操作

### 1. ==选举机制（面试重点）==

![image-20230517142213912](.assets/image-20230517142213912.png)

![image-20230517142312105](.assets/image-20230517142312105.png)



### ZK集群==启停脚本==

1. 在 node1 的/home/luoyi/bin 目录下创建脚本

```shell
[root@node1 bin]# vim zk.sh
```

2. 脚本内容

```shell
#!/bin/bash
case $1 in
"start"){
for i in node1 node2 node3
do
 echo ---------- zookeeper $i 启动 ------------
ssh $i "/export/server/zookeeper/bin/zkServer.sh 
start"
done
};;
"stop"){
for i in node1 node2 node3
do
 echo ---------- zookeeper $i 停止 ------------ 
ssh $i "/export/server/zookeeper/bin/zkServer.sh 
stop"
done
};;
"status"){
for i in node1 node2 node3
do
 echo ---------- zookeeper $i 状态 ------------ 
ssh $i "/export/server/zookeeper/bin/zkServer.sh 
status"
done
};;
esac
```

3. 增加脚本执行权限

```shell
chmod 777 zk.sh
```

4. ==启动脚本==

```shell
[root@node1 bin]# ./zk.sh start
```

5. ==停止脚本==

```shell
[root@node1 bin]# ./zk.sh stop
```





### 2. ==客户端命令行操作==

#### 2.1 命令行语法

![image-20230517145548260](.assets/image-20230517145548260.png)

![image-20230517145601195](.assets/image-20230517145601195.png)

1. ==启动客户端==

```shell
[root@node1 zookeeper]# bin/zkCli.sh -server node1:2181
```

2. 显示所有操作命令

```shell
[zk: node1:2181(CONNECTED) 0] help
```



#### 2.2 znode节点数据信息

##### 2.2.1 查看当前znode包含的内容

```shell
[zk: node1:2181(CONNECTED) 1] ls /
```

##### 2.2.2 查看当前节点详细数据

```shell
[zk: node1:2181(CONNECTED) 5] ls -s /
[hbase, zookeeper]cZxid = 0x0
ctime = Thu Jan 01 08:00:00 CST 1970
mZxid = 0x0
mtime = Thu Jan 01 08:00:00 CST 1970
pZxid = 0x200000002
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 0
numChildren = 2
```

（1）==**czxid：创建节点的事务 zxid**== 每次修改 ZooKeeper 状态都会产生一个 ==**ZooKeeper 事务 ID**==。事务 ID 是 ZooKeeper 中所 有修改总的次序。**每次修改都有唯一的 zxid**，如果 zxid1 小于 zxid2，那么 zxid1 在 zxid2 之 前发生。 

（2）ctime：znode 被创建的毫秒数（从 1970 年开始） 

（3）mzxid：znode 最后更新的事务 zxid 

（4）mtime：znode 最后修改的毫秒数（从 1970 年开始） 

（5）pZxid：znode 最后更新的子节点 zxid

（6）cversion：znode 子节点变化号，znode 子节点修改次数 

**==（7）dataversion：znode 数据变化号==**

（8）aclVersion：znode 访问控制列表的变化号 

（9）ephemeralOwner：如果是临时节点，这个是 znode 拥有者的 session id。如果不是 临时节点则是 0。 

**==（10）dataLength：znode 的数据长度==**

==（11）numChildren：znode 子节点数量==



#### 2.3 节点类型（持久/短暂/有序号/无序号）

![image-20230517151235780](.assets/image-20230517151235780.png)



##### 2.3.1 分别创建2个普通节点（持久+无序）

```shell
//创建节点时，要赋值
[zk: localhost:2181(CONNECTED) 3] create /sanguo "diaochan"

[zk: localhost:2181(CONNECTED) 4] create /sanguo/shuguo 
"liubei"

```

##### 2.3.2 获得节点值

```shell
[zk: localhost:2181(CONNECTED) 5] get -s /sanguo
diaochan
cZxid = 0x100000003
ctime = Wed Aug 29 00:03:23 CST 2018
mZxid = 0x100000003
mtime = Wed Aug 29 00:03:23 CST 2018
pZxid = 0x100000004
cversion = 1
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 7
numChildren = 1

[zk: localhost:2181(CONNECTED) 6] get -s /sanguo/shuguo
liubei
cZxid = 0x100000004
ctime = Wed Aug 29 00:04:35 CST 2018
mZxid = 0x100000004
mtime = Wed Aug 29 00:04:35 CST 2018
pZxid = 0x100000004
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 6
numChildren = 0
```

##### 2.3.3 创建带序号的节点（持久+有序）

1. 先创建一个普通的根节点/sanguo/weiguo

```shell
[zk: localhost:2181(CONNECTED) 1] create /sanguo/weiguo 
"caocao"
```

2. 创建带序号的节点

   如果原来没有序号节点，序号从 0 开始依次递增。如果原节点下已有 2 个节点，则再排 序时从 2 开始，以此类推。

   ```shell
   [zk: localhost:2181(CONNECTED) 2] create -s 
   /sanguo/weiguo/zhangliao "zhangliao"
   Created /sanguo/weiguo/zhangliao0000000000
   [zk: localhost:2181(CONNECTED) 3] create -s 
   /sanguo/weiguo/zhangliao "zhangliao"
   Created /sanguo/weiguo/zhangliao0000000001
   [zk: localhost:2181(CONNECTED) 4] create -s 
   /sanguo/weiguo/xuchu "xuchu"
   Created /sanguo/weiguo/xuchu0000000002
   ```



##### 2.3.4 创建短暂节点（短暂节点 + 不带序号 or 带序号）

1. 创建短暂的不带序号的节点

```shell
[zk: localhost:2181(CONNECTED) 7] create -e /sanguo/wuguo 
"zhouyu"
```

2. 创建短暂的带序号的节点

```shell
[zk: localhost:2181(CONNECTED) 2] create -e -s /sanguo/wuguo 
"zhouyu"
Created /sanguo/wuguo0000000001
```

3. 在当前客户端是能查看到的

```shell
[zk: localhost:2181(CONNECTED) 3] ls /sanguo 
[wuguo, wuguo0000000001, shuguo]
```

4. 退出当前客户端然后再重启客户端

```shell
[zk: localhost:2181(CONNECTED) 12] quit
[atguigu@hadoop104 zookeeper-3.5.7]$ bin/zkCli.sh
```

5. 再次查看根目录下短暂节点已经删除

```shell
[zk: localhost:2181(CONNECTED) 0] ls /sanguo
[shuguo]
```



##### 2.3.5 修改节点数据值

```shell
[zk: localhost:2181(CONNECTED) 6] set /sanguo/weiguo "simayi"
```





#### 2.4 ==监听器原理==

客户端注册监听它关心的目录节点，**当目录节点发生变化（数据改变、节点删除、子目 录节点增加删除）时，ZooKeeper 会通知客户端。**监听机制<u>保证 ZooKeeper 保存的任何的数据的任何改变都能快速的响应到监听了该节点的应用程序</u>。

![image-20230517152813645](.assets/image-20230517152813645.png)



##### 2.4.1 节点的值变化监听

<u>在hadoop103再多次修改/sanguo的值，hadoop104上不会再收到监听。因为注册 一次，只能监听一次。想再次监听，需要再次注册。</u>

1. 在 hadoop104 主机上注册监听/sanguo 节点数据变化

```shell
[zk: localhost:2181(CONNECTED) 26] get -w /sanguo
```

2. 在 hadoop103 主机上修改/sanguo 节点的数据

```shell
[zk: localhost:2181(CONNECTED) 1] set /sanguo "xisi"
```

3. 观察 hadoop104 主机收到数据变化的监听

```shell
WATCHER::
WatchedEvent state:SyncConnected type:NodeDataChanged 
path:/sanguo
```



##### 2.4.2 节点的子节点变化监听（路径变化）

<u>节点的路径变化，也是注册一次，生效一次。想多次生效，就需要多次注册。</u>

1. 在 hadoop104 主机上注册监听/sanguo 节点的子节点变化

```shell
[zk: localhost:2181(CONNECTED) 1] ls -w /sanguo
[shuguo, weiguo]
```

2. 在 hadoop103 主机/sanguo 节点上创建子节点

```shell
[zk: localhost:2181(CONNECTED) 2] create /sanguo/jin "simayi"
Created /sanguo/jin
```

3. 观察 hadoop104 主机收到子节点变化的监听

```shell
WATCHER::
WatchedEvent state:SyncConnected type:NodeChildrenChanged 
path:/sanguo
```





#### 2.5 节点删除与查看

##### 2.5.1 删除节点

```shell
[zk: localhost:2181(CONNECTED) 4] delete /sanguo/jin
```

##### 2.5.2 递归删除节点

```shell
[zk: localhost:2181(CONNECTED) 15] deleteall /sanguo/shuguo
```

##### 2.5.3 查看节点状态

```shell
[zk: localhost:2181(CONNECTED) 17] stat /sanguo
cZxid = 0x100000003
ctime = Wed Aug 29 00:03:23 CST 2018
mZxid = 0x100000011
mtime = Wed Aug 29 00:21:23 CST 2018
pZxid = 0x100000014
cversion = 9
dataVersion = 1
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 4
numChildren = 1
```





### 3. 客户端API操作

#### 3.1 IDEA环境搭建

1. 创建一个工程
2. 添加pom文件

```xml
<dependencies>
	<dependency>
		<groupId>junit</groupId>
		<artifactId>junit</artifactId>
		<version>RELEASE</version>
	</dependency>
	<dependency>
		<groupId>org.apache.logging.log4j</groupId>
		<artifactId>log4j-core</artifactId>
		<version>2.8.2</version>
	</dependency>
	<dependency>
		<groupId>org.apache.zookeeper</groupId>
		<artifactId>zookeeper</artifactId>
		<version>3.5.7</version>
	</dependency>
</dependencies>
```

3. 拷贝log4j.properties文件到项目根目录

需要在项目的 src/main/resources 目录下，新建一个文件，命名为“log4j.properties”，在 文件中填入。

```xml
log4j.rootLogger=INFO, stdout 
log4j.appender.stdout=org.apache.log4j.ConsoleAppender 
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout 
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] 
- %m%n 
log4j.appender.logfile=org.apache.log4j.FileAppender 
log4j.appender.logfile.File=target/spring.log 
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout 
log4j.appender.logfile.layout.ConversionPattern=%d %p [%c] 
- %m%n
```

4. 创建包名com.atguigu.zk
5. 创建类名称zkClient



#### 3.2 创建Zookeeper客户端

```java
// 注意：逗号前后不能有空格
private static String connectString =
"node1:2181,node2:2181,node3:2181";
private static int sessionTimeout = 2000;
private ZooKeeper zkClient = null;
@Before
public void init() throws Exception {
zkClient = new ZooKeeper(connectString, sessionTimeout, new 
Watcher() {
@Override
public void process(WatchedEvent watchedEvent) {
// 收到事件通知后的回调函数（用户的业务逻辑）
System.out.println(watchedEvent.getType() + "--" 
+ watchedEvent.getPath());
// 再次启动监听
try {
List<String> children = zkClient.getChildren("/", 
true);
 for (String child : children) {
 System.out.println(child);
 }
} catch (Exception e) {
e.printStackTrace();
}
}
});
}
}
```



#### 3.3 创建子节点

```java
// 创建子节点
@Test
public void create() throws Exception {
// 参数 1：要创建的节点的路径； 参数 2：节点数据 ； 参数 3：节点权限 ；
参数 4：节点的类型
String nodeCreated = zkClient.create("/atguigu", 
"shuaige".getBytes(), Ids.OPEN_ACL_UNSAFE,
CreateMode.PERSISTENT);
}
```



#### 3.4 获取子节点并监听节点变化

```java
// 获取子节点
@Test
public void getChildren() throws Exception {
List<String> children = zkClient.getChildren("/", true);
for (String child : children) {
System.out.println(child);
}
// 延时阻塞
Thread.sleep(Long.MAX_VALUE);
}
```

（1）在 IDEA 控制台上看到如下节点： 

```shell
zookeeper 
sanguo 
atguigu 
```

（2）在 hadoop102 的客户端上创建再创建一个节点/atguigu1，观察 IDEA 控制台 

```shell
[zk: localhost:2181(CONNECTED) 3] create /atguigu1 "atguigu1" 
```

（3）在 hadoop102 的客户端上删除节点/atguigu1，观察 IDEA 控制台 

```shell
[zk: localhost:2181(CONNECTED) 4] delete /atguigu1
```



#### 3.5 判断Znode是否存在

```java
// 判断 znode 是否存在
@Test
public void exist() throws Exception {
Stat stat = zkClient.exists("/atguigu", false);
System.out.println(stat == null ? "not exist" : "exist");
}

```





### 4. 客户端向服务端写数据流程

![image-20230517154110961](.assets/image-20230517154110961.png)

![image-20230517154118186](.assets/image-20230517154118186.png)





## 四、服务器动态上下线监听案例

### 1. 需求

某分布式系统中，主节点可以有多台，可以动态上下线，任意一台客户端都能实时感知 到主节点服务器的上下线。





### 2. 需求分析

![image-20230519104020937](.assets/image-20230519104020937.png)





### 3. 代码实现

1. 在集群上创建==**/servers 节点**==

```shell
[zk: node1:2181(CONNECTED) 1] create /servers "servers"
```

2. 在 Idea 中创建包名：com.atguigu.zkcase1
3. ==服务器端==向 Zookeeper 注册代码

```java
public class DistributeServer {
    private String connectString = "node1:2181,node2:2181,node3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        DistributeServer server = new DistributeServer();
        //1. 获取zk连接
        server.getConnect();
        //2. 注册服务器到zk集群
        server.regist(args[0]);
        //3. 启动业务逻辑（睡觉）
        server.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist(String hostname) throws InterruptedException, KeeperException {
        String create = zk.create("/servers", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online");
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
```

4. 客户端代码

```java
public class DistributeClient {
    private String connectString = "node1:2181,node2:2181,node3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeClient client = new DistributeClient();
        //1. 获取zk连接
        client.getConnect();
        //2. 监听/server 子节点路径变化
        client.getServerList();
        //3. 业务逻辑（睡觉）
        client.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren("/servers", true);

        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zk.getData("/servers/" + child, false, null);
            servers.add(new String(data));
        }
        
        //打印
        System.out.println(servers);
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
```





### 4. 测试

#### 4.1 在Linux上操作增加减少服务器

1. 启动 DistributeClient 客户端
2. 在 node1 上 zk 的客户端/servers 目录上创建临时带序号节点

```shell
[zk: localhost:2181(CONNECTED) 1] create -e -s 
/servers/hadoop102 "hadoop102"
[zk: localhost:2181(CONNECTED) 2] create -e -s 
/servers/hadoop103 "hadoop103"
```

3. 观察 Idea 控制台变化

```java
[hadoop102, hadoop103]
```

4. 执行删除操作

```shell
[zk: localhost:2181(CONNECTED) 8] delete 
/servers/hadoop1020000000000
```

5. 观察 Idea 控制台变化

```java
[hadoop103]
```



#### 4.2 在IDEA上操作增加减少服务器

1. 启动 DistributeClient 客户端（如果已经启动过，不需要重启）

2. 启动 DistributeServer 服务

   1. 点击 Edit Configurations…

   ![image-20230519170434989](.assets/image-20230519170434989.png)

   2. 在弹出的窗口中（Program arguments）输入想启动的主机，例如，node1

   ![image-20230519170457304](.assets/image-20230519170457304.png)

   3. 回到 DistributeServer 的 main 方 法 ， 右 键 ， 在 弹 出 的 窗 口 中 点 击 Run “DistributeServer.main()”

   ![image-20230519170535565](.assets/image-20230519170535565.png)

   4. 观察 DistributeServer 控制台，提示 node1 is working

   5. 观察 DistributeClient 控制台，提示 node1 已经上线





## 五、 ZooKeeper 分布式锁案例

==**"进程 1"在使用该资源的时候，会先去获得锁，"进程 1"获得锁以后会对该资源保持独占**==，这样其他进程就无法访问该资源，==**"进程 1"用完该资源以后就将锁释放掉，让其 他进程来获得锁**==，那么通过这个锁机制，我们就能保证了分布式系统中多个进程能够有序的 访问该临界资源。那么我们把这个分布式环境下的这个锁叫作分布式锁。

![image-20230519170751139](.assets/image-20230519170751139.png)



### 1. 原生 Zookeeper 实现分布式锁案例

#### 1.1 分布式锁实现

```java
public class DistributeLock {
    private final String connectString = "node1:2181,node2:2181,node3:2181";
    private final int sessionTimeout = 3000;
    private final ZooKeeper zk;

    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath;
    private String currentMode;

    public DistributeLock() throws IOException, InterruptedException, KeeperException {
        //获取连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //connectLatch 如果连接上zk 可以释放
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }
                //waitLatch 需要释放
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getType().equals(waitPath)) {
                    waitLatch.countDown();
                }

            }
        });

        //等待zookeeper连接上
        connectLatch.await();

        //判断根节点/locks是否存在
        Stat stat = zk.exists("/locks", false);

        if (stat == null) {
            //创建根节点
            zk.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        }

    }

    //对zk加锁
    public void zklock() {
        //创建对应的临时带序号节点
        try {
            currentMode = zk.create("/locks" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            //判断创建的节点是否为最小的序号节点，是则获取到锁，不是则监听该序号前一个节点
            List<String> children = zk.getChildren("/locks", false);
            //如果children只有一个值则直接获取锁，若有多个节点需要判断谁最小
            if (children.size() == 1) {
                return;
            } else {
                Collections.sort(children);

                //获取对应的节点名称 seq-00000000
                String thisNode = currentMode.substring("/locks/".length());
                //通过seq-00000000 获取该节点在children集合的位置
                int index = children.indexOf(thisNode);

                //判断
                if (index == -1) {
                    System.out.println("数据异常");
                } else if (index == 0) {
                    //就一个节点可以获取锁了
                    return;
                } else {
                    //需要监听前一个节点的变化
                    waitPath = "/locks/" + children.get(index - 1);
                    zk.getData("waitPath", true, null);

                    //等待监听
                    waitLatch.await();
                    return;
                }
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    //解锁
    public void unzklock() {
        //删除节点
        try {
            zk.delete(currentMode, -1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
```



#### 1.2 分布式锁测试

1. 创建两个线程

```java
public class DistributeLockTest {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        final DistributeLock lock1 = new DistributeLock();

        final DistributeLock lock2 = new DistributeLock();

        new Thread(new Runnable() {
            @Override
            public void run() {


                try {
                    lock1.zklock();
                    System.out.println("线程1 启动，获取到锁");
                    Thread.sleep(5 * 1000);
                    lock1.unzklock();
                    System.out.println("线程1 释放锁");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {


                try {
                    lock2.zklock();
                    System.out.println("线程2 启动，获取到锁");
                    Thread.sleep(5 * 1000);
                    lock2.unzklock();
                    System.out.println("线程2 释放锁");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }
}
```

2. 观察控制台变化





### ==2. Curator 框架实现分布式锁案例==（便捷推荐）

#### 2.1 原生的 Java API ==开发存在的问题==

1. 会话连接是异步的，需要自己去处理。比如使用 CountDownLatch

2. Watch 需要重复注册，不然就不能生效
3. 开发的复杂性还是比较高的
4. 不支持多节点删除和创建。需要自己去递归



#### 2.2 Curator 是一个专门解决分布式锁的框架，解决了原生 JavaAPI 开发分布式遇到的问题。

详情请查看官方文档：https://curator.apache.org/index.html



#### 2.3 Curator 案例实操

1. 添加依赖

```xml
<dependency>
 <groupId>org.apache.curator</groupId>
 <artifactId>curator-framework</artifactId>
 <version>4.3.0</version>
</dependency>
<dependency>
 <groupId>org.apache.curator</groupId>
 <artifactId>curator-recipes</artifactId>
 <version>4.3.0</version>
</dependency>
<dependency>
 <groupId>org.apache.curator</groupId>
 <artifactId>curator-client</artifactId>
 <version>4.3.0</version>
</dependency>
```

2. 代码实现

```java
public class CuratorLockTest {
    public static void main(String[] args) {

        //创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");

        //创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");

        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    lock1.acquire();
                    System.out.println("线程1 获取到锁");

                    Thread.sleep(5 * 1000);
                    lock1.release();
                    System.out.println("线程1 释放锁");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    lock2.acquire();
                    System.out.println("线程2 获取到锁");

                    Thread.sleep(5 * 1000);
                    lock2.release();
                    System.out.println("线程2 释放锁");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    private static CuratorFramework getCuratorFramework() {

        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);

        CuratorFramework client = (CuratorFramework) CuratorFrameworkFactory.builder().connectString("node1:2181,node2:2181,node3:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy).build();

        //启动客户端
        client.start();
        System.out.println("zookeeper 启动成功");
        return client;
    }
}
```





## 六、 企业面试真题

### 1. 选举机制

**半数机制，超过半数的投票通过，即通过。** 

1. 第一次启动选举规则： 
   - ==投票过半数时，服务器 id 大的胜出== 

2. 第二次启动选举规则： 
   1. ==EPOCH 大的直接胜出== 
   2. ==EPOCH 相同，事务 id 大的胜出==
   3. ==事务 id 相同，服务器 id 大的胜出==





### 2. 生产集群安装多少 zk 合适？

​	==**安装奇数台**==

生产经验：

- 10 台服务器：3 台 zk
- 20 台服务器：5 台 zk
- 100 台服务器：11 台 zk
- 200 台服务器：11 台 zk



**==服务器台数多：好处，提高可靠性；坏处：提高通信延时==**



### 3. 常用命令

**ls、get、create、delete**







# Zookeeper源码分析

## 一、 算法基础

### 1. 拜占庭将军问题





### 2. Paxos算法





### 3. ZAB协议





### 4. CAP







## 二、 源码详解





















