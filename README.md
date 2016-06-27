# MapReduce
清华大学大数据作业MapReduce处理用户微博JSON数据
# Hadoop Experiment
1. 输入数据

输入数据文件已保存在了Hadoop的HDFS(Hadoop Distributed File System)下的目录/input-user中（关于Hadoop的使用参见第5节）。输入数据是经过了压缩的json文件，Hadoop会自动处理压缩格式的输入数据，所以你不需要进行额外的解压缩处理。

输入数据以json格式存储，每行是一条微博数据，或者是一条用户信息数据（你需要在程序中进行判断并分别处理）。每条微博数据格式如下：

{ "_id" : XX, "user_id" : XXXXXXXX, "text" : "XXXXXXXXXXXXXX", "created_at" : "XXXXXX" }

本实验中我们只需使用user_id项，该项是发微博用户的id。 

每个用户信息格式如下：

{ "_id" : XXXXXX, "name" : "XXXX" }

本实验中我们两项都会用到，_id是该用户的id，name是该用户的用户名。

输入数据的样例请见本页最后一部分“输入数据格式样例”。

2. 输出数据

本实验要求输出每个用户名及他发微薄的条数，按数量从大到小排序，示例如下：

86    贫苦少年王笨笨

82    孙子悦是CC不是cc

79    基德丨鳞

75    糖醋鱼刺

每行数目和用户名的中间用制表符（\t）隔开。

3. 程序模板

具体见模板UserCount.java。请仔细阅读模板中的内容，其中有许多提示。也可以参考Hadoop官网上的WordCount样例程序。

4. 编译程序

编译时请进入虚拟机中的/home/ubuntu/project目录，然后输入：

sh compile.sh

这会对.java文件进行编译，并自动把java程序压缩成jar包。之后该目录下会出现一个叫做UserCount.jar的文件，这就是我们运行Hadoop程序时所要用的文件。

即使编译错误，脚本也会自动生成一个jar包，这个jar包是无法提交给Hadoop执行的，请确保没有编译错误再进行之后的步骤。

5. 运行程序

进入虚拟机的/home/ubuntu/project目录下，运行命令：

sh start-cluster.sh

会自动启动Hadoop的dfs服务和yarn服务。此时整个Hadoop集群应该已经启动，你可以通过jps命令查看正在运行的java进程，正常情况下应至少包括如下4个：Jps, NameNode, SecondaryNameNode, ResourceManager。

要运行刚才编译好的jar包，请在/home/ubuntu/project目录中运行命令：

sh run-job.sh

大家如果对Hadoop的使用比较熟悉，也可以自己使用Hadoop命令提交任务，Hadoop安装在/home/ubuntu/hadoop-2.7.1目录中。但是请务必使用start-cluster.sh启动Hadoop集群，因为我在启动前需要对集群进行一些初始化。

注意，重复运行任务时，如果HDFS中还保存着上一次运行时生成的结果文件或临时中间文件，运行时会报文件冲突错误，需要提前把有冲突的目录或文件删除，具体删除方法见后面的Hadoop使用参考。

如果大家对于Hadoop的运行有更多的问题，可以参考WordCount样例程序或MapReduce教程。 

6. 计算结果

计算的结果存在HDFS的哪个目录下取决于你的java程序。把结果从HDFS上复制到虚拟机的本地路径中可以使用:

/home/ubuntu/hadoop-2.7.1/bin/hadoop fs –copyToLocal /path/on/hdfs /path/on/master

其中/path/on/hdfs是结果文件在HDFS上的路径，/path/on/master是你希望用于存放计算结果的本地路径。

之后在/path/on/master目录下会有一个叫part-r-00000或是partXXX的文件，这个就是计算的结果。

如果结果没有问题，请将结果文件重命名为result.txt，并放置在/home/ubuntu/project文件夹中。运行 sh submit.sh 会自动将你的代码和result.txt提交到评分服务器暂存（你也可以通过该方式保存未完成的代码）。点击作业提交页面的提交按钮，服务器会检查你的结果是否正确并给出评分。

Hadoop使用参考

Hadoop安装在虚拟机的/home/ubuntu/hadoop-2.7.1目录中，我们可以在/home/ubuntu/hadoop-2.7.1/bin目录中运行Hadoop命令。

./hadoop fs -ls /path/on/hdfs : 列出HDFS上某目录的内容，类似于本地的ls命令
./hadoop fs -rm /path/on/hdfs : 删除HDFS上某个文件
./hadoop fs -rm -r /path/on/hdfs : 删除HDFS上某个目录
./hadoop fs -cat /path/on/hdfs : 查看HDFS上某个文件的内容，不过HDFS上的文件一般体积都比较大，推荐使用./hadoop fs -cat /path/on/hdfs | head -n 10 查看文件前10行
./hadoop fs -copyToLocal /path/on/hdfs /path/on/local : 把HDFS上的文件拷贝到虚拟机本地
./yarn application -list : 列出当前正在执行的任务，可以使用这个命令查询任务执行进度
./yarn application -kill <JobId> : 终止某个正在执行的任务
JSON格式文件的处理

对于json格式的输入文件，不需要手写一个程序来读取这些项，你可以使用org.json包中的JSONObject类来处理，具体使用办法：

String s = "{ \"_id\" : XX, \"user_id\" : XXXXXXXX, \"text\" : \"XXXXXXXXXXXXXX\", \"created_at\" : \"XXXXXX\" }"

JSONObject js = new JSONObject(s);

然后就可以从这个JSONObject中提取你需要的项，比如：

String id = null;

if ( js.has(“user_id”))

    id = js.optString(“user_id”);

切记在使用optString()前要用has()来检查该项是否存在，否则程序可能直接出错中断。

另外，JSONObject js = new JSONObject(s); 以及 id = js.optString(“id”); 语句会产生JSONException，请使用try-catch语句或者添加抛出异常。

输入数据格式样例

{ "_id" : 376049, "user_id" : 1643097417, "text" : "苹果跟联通合作，iphone来了；谷歌平台跟移动结婚，ophone来了；电信干什么去了，黑莓要来了么？？", "created_at" : "Tue Sep 01 08:38:17 +0800 2009" }

{ "_id" : 376134, "user_id" : 1223277140, "text" : "九月第一天，我却辞职了!", "created_at" : "Tue Sep 01 08:41:57 +0800 2009" }

{ "_id" : 376244, "user_id" : 1639065174, "text" : "秋天终于来了天气要向冷开始转换了,偶最不愿意看到的结果", "created_at" : "Tue Sep 01 08:46:52 +0800 2009"  }

{ "_id" : 376336, "user_id" : 1640712172, "text" : "08:47北二环德胜门桥至小街桥西向东,钟楼北桥至西直门东向西，车辆行驶缓慢。  http://sinaurl.cn/h5daT", "created_at" : "Tue Sep 01 08:50:33 +0800 2009" }

{ "_id" : 1886796931, "name" : "叙Carens" }

{ "_id" : 1374588365, "name" : "畅畅bella" }

{ "_id" : 1784427554, "name" : "我想考二本" }

{ "_id" : { "$numberLong" : "2372210700" }, "name" : "王栎鑫暴走贝小妞" }

{ "_id" : { "$numberLong" : "2253254920" }, "name" : "Granville丶" }

{ "_id" : 1915163215, "name" : "小雅向前冲" }

#Spark Experiment
输入数据

输入数据已经保存在了HDFS中：hdfs://192.168.70.141:8020/Assignment1。其中每行代表一篇文章，格式为：

doc_id, doc_content

文章样例：

317,newsgroups rec motorcyclespath cantaloupe srv cs cmu rochester cornell batcomputer caen uwm linac uchinews quads geafrom gea quads uchicago gerardo enrique arnaez subject bike message apr midway uchicago sender news uchinews uchicago news system reply gea midway uchicago eduorganization university chicagodate mon apr gmtlines honda shadow intruder heard bikes plan time riding twin cruisers bikes dont massive goldw

输出数据

本次实验要求统计所有单词的Document Frequency，包含该单词的文章数。例如某个单词DF=10，表示共有10篇文章包括这个单词。

实验要求以JSON格式输出所有DF=10的单词及其对应的倒排表键值，每行对应一个单词，格式定义如下：

{w1: [ { w1_d1: [ w1_d1_p1, d1_p2 ] },{ w1_d2: [ w1_d2_p1 ] },{ w1_d3: [ w1_d3_p1 ] } ]}  

{w2: [ { w2_d1: [ w1_d1_p1, d1_p2, d1_p3] },{ w2_d2: [ w1_d2_p1] } ]}  

如上倒排表所示，单词w1出现在三篇文档中，文档的ID分别：w1_d1、w1_d2、w1_d3。该单词w1在文档w1_d1中出现了两次，出现的位置分别为w1_d1_p1和d1_p2，在文档w1_d2和w1_d3中各出现一次，其出现位置分别为w1_d2_p1和w1_d3_p1。 输出样例如下：

{"circle":[{"642":[136] },{"120":[165] },{"1796":[75] },{"1862":[168] },{"611":[210] },{"646":[37] },{"519":[150] },{"1469":[944] },{"558":[108] },{"1463":[102] }]}  

请把结果用println函数打印到标准输出。

程序模板

程序模板见project目录下，src/main/scala/com/dataman/demo/Assignment1.scala。 使用在线编辑器可以直接编辑该文件。

编译程序

我们提供了两种编译方式，如果 sbt 编译速度较慢，可以尝试用 maven 编译。在虚拟机的project文件夹下执行下面的命令。

sbt:

sbt clean assembly

如果编译成功，编译结果保存在 target/scala-2.10/spark-demo-assembly-1.0.jar

maven:

mvn clean package

如果编译成功，编译结果保存在 target/spark-demo-1.0.jar

运行程序

本次实验需要在docker container中运行spark程序，如果对container的概念不是很了解，建议先阅读https://en.wikipedia.org/wiki/LXC。

1. 启动 docker container：

docker run -it --net host -v /home/ubuntu/project:/tmp/project offlineregistry.dataman-inc.com/tsinghua/spark:1.5.0-hadoop2.6.0 bash

该指令会创建一个 spark 的 docker container，并进入 container 的 /opt/spark/dist 目录，这也是 spark 的 home 目录。同时，将外部的 /home/ubuntu/project 挂载到了 container 内部的 /tmp/project 目录下，你可以在这里找到你的编译结果。

2. 运行 spark：

在 Spark 目录下，也就是container中的/opt/spark/dist，运行如下指令：

bin/spark-submit --jars /tmp/project/target/spark-demo-1.0.jar --class com.dataman.demo.Assignment1 /tmp/project/target/spark-demo-1.0.jar > /tmp/project/data/answer

运行结果会被重定向到 project 下的 data/answer 文件中

3.  退出 docker 请使用 ctrl+p ctrl+q，这样可以确保 docker container 不会被停止。如果需要重新进入 docker，请使用 docker attach (containerID)，其中 containID 通过docker ps 查看。
