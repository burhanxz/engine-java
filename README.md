# engine-java
阿里天池 第一届POLARDB数据库性能大赛.Java排名前五.源码<br>
比赛代码，前后换了好几种实现，写得也比较匆忙。<br>
最终选择的是GroupDB.java中的实现<br>

# 赛题
地址：https://tianchi.aliyun.com/programming/introduction.htm?spm=5176.100066.0.0.23fc33af6BO79i&raceId=231689<br>
实现一个简化、高效的kv存储引擎，支持Write、Read、Range接口<br>
1）Recover正确性评测：
- 此阶段评测程序会并发写入特定数据（key 8B、value 4KB）同时进行任意次kill -9来模拟进程意外退出（参赛引擎需要保证进程意外退出时数据持久化不丢失），接着重新打开DB，调用Read、Range接口来进行正确性校验

2）性能评测
-  随机写入：64个线程并发随机写入，每个线程使用Write各写100万次随机数据（key 8B、value 4KB）
-  随机读取：64个线程并发随机读取，每个线程各使用Read读取100万次随机数据
-  顺序读取：64个线程并发顺序读取，每个线程各使用Range有序（增序）遍历全量数据2次


# 总体思路
- 随机写入阶段，改进WAL思想，让key和value分离存储。将key(8B)的高十位作为分段依据，把传入的key和value分发到1024个分区上。其中，key使用mmap的方式序列化到硬盘上的1024个文件中，而value使用dio序列化。value和key在硬盘上的位置是一一对应的，避免写入value的地址。

- 随机读取阶段，把硬盘中的key数据全部载入内存，建立索引。索引包含key分区+value地址信息。之前的版本采用加强版LongLongHashMap作为索引容器，利用分段锁和CAS实现并发缓存索引；最终版本使用了一个分区一个hashmap的方案来实现并发导入。随机读取的时候，根据key获取value地址，再使用dio从硬盘读出value。

- 顺序读取阶段，采用生产者（缓存value）-消费者(visit缓存)模式。生产者负责缓存value。在2.5GB总内存中，建立5个256MB的堆外内存，分别由5个线程控制，每份堆外内存可以缓存一个分区的value，形成一个大小为5的滑动窗口，并发缓存1024个分区。消费者执行顺序visit，使用生产者缓存的数据，使用完成后清空堆外内存
