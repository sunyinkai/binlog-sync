## README

### 模拟binlog同步

#### 思路

**1**

​	最基础的做法,把csv文件解析出来，一条一条插入或者删除即可。

**2**

​	为了提高速度，考虑把多个Insert合并在一起。比如:
​	  (id  val status)
​	  1   2    I,
​	  1   3    I,
​	  1   4    I,

​	这3个操作可以只保留最后一个操作。

​	但是也不要一次操作包含太多行，因为行数越多出故障的概率越大。比如DB对行数的限制，机器突然挂掉后需要重做的内容太多。

**3b**

​	暂未实现。

**3c**

​	在下游新建一张表pos，记录(table_name,line)表示表table_name已经同步到line行了。每次把数据的导入数据库时同时把pos表对应的record更新。用事务保证这两个操作的原子性。
​	为了实现断点续传，只需要先读取pos表，获取到上次同步到的位置，然后继续往下同步即可。

#### 测试

100w数据插入：          耗时：72s


再将刚才100w数据删除：      耗时：74s
