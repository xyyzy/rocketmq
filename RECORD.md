https://ata.atatech.org/articles/12020324907?spm=ata.23639746.0.0.435d36fa4pCKqU#NTNmZDk1
设计理念
● RocketMQ设计是基于主题的发布与订阅模式，核心功能包括：消息发送、消息存储、消息消费；
整体设计主要特性可以概括为：简单、安全、高性能、最终一致性
● 简单：引入Nameserver作为协调组件，与业界普通使用Zookeeper作为协调组件不同，RocketMQ通过自研Nameserver来实现元数据的管理（Topic路由信息等）。RocketMQ从实际需求出发，因为Topic路由信息无须在集群之间保持强一致，而追求的是最终一致性，并且能容忍分钟级的不一致，所以RocketMQ的Nameserver集群间是互不通信的。基于此种情况，既可以降低Nameserver实现复杂度，也降低了对网络的要求，而性能却比使用Zookeeper要高。
● 高性能：
○ 高效的IO存储机制。RocketMQ追求消息发送的高吞吐量，RocketMQ的消息存储文件设计成文件组的概念，组内单个文件大小固定（CommitLog 1G，ConsumeQueue 600W个字节，IndexFile （40 + 500W*4 + 2000W*20）个字节），方便引入内存映射机制(mmap)，所有的主题消息都是基于顺序写，这样可以极大提升消息写性能，同时为了兼顾消息消费与消息查找，又引入了消息消费队列（ConsumeQueue）和索引文件(IndexFile)
○ 异步编程。RocketMQ源码几乎都是异步处理逻辑，大量使用CompletedFuture虽然代码某些地方可读性不高，但在代码层面确实提升效率。
● 安全：多选的Broker刷盘机制。RocketMQ是金融级消息中间件，在Broker刷盘时提供了三种机制，同步刷盘、异步刷盘、异步提交，三种方式在性能和安全之间做取舍，如果保障消息不丢失，使用同步刷盘，如果能容忍一定数据丢失追求性能可以使用异步提交。
● at least once。在使用消息中间件时，通常会遇到一个问题，就是如何保证消息一定会被消费者消费，并且只被消费一次。RocketMQ解决方案是：不解决，谁用谁负责处理。因为这样将极大简化RocketMQ的内核实现，并且可以保证发送简单与高效的高可用消息，消息重复问题由消费者在消费时自己实现幂等。
○ at least once，由业务保证幂等消费贯穿整个rockemq设计，产生场景：
■ Producer重复发送
■ rebalance
■ 主从模式
■ Consumer消费失败
○ 这也是架构的设计哲学，如果由RocketMQ保证exactly once，虽然业务接入不用考虑幂等处理，但是会带来方案的不确定性，同时给RocketMQ增加极高复杂度，而由业务自己保证并不会带来过高的复杂度。可以得到启发方案明确且通用的功能下沉至平台，如果平台逻辑复杂而业务实现简单，可以让业务自己实现，降低平台复杂度。
● 方案设计前后一致。RocketMQ很多设计方案的思路都是相近的。
○ 例如延迟消息、事务消息、失败消息重试的设计，均通过不影响原消息逻辑的前提下，将消息投递至改写的opic、queue，在新的topic中做特殊逻辑处理，完成后清除标记重新投递原始topic、queue，通过这种旁路手段，在不影响原始逻辑情况下能扩展新逻辑。
○ 业务保证幂等：在业务保证幂等前提下，很多一致性问题得到简化，如果不确定是否成功一律按照失败重试，例如，consumer offset消费位点的维护。
故障机制
发生时间点：故障延迟机制指的是在发送消息时记录每个Broker的耗时时间，如果某个Broker发生故障，但是生产者还未感知（NameServer 30s检测一次心跳，有可能Broker已经发生故障但未到检测时间，所以会有一定的延迟），用耗时时间做为一个故障规避时间（也可以是30000ms）此时消息会发送失败，在重试或者下次选择消息队列的时候，可以避免再次选择该Broker。
如果某个Topic所在的所有Broker都处于不可用状态，此时尽量选择延迟时间最短、规避时间最短（排序后的失败条目中靠前的元素）的Broker作为此次发送消息消息的Broker。



消息事物

半消息：生产者发送事物消息的时候，由mq框架管理的一个消息中间状态，半消息发送出，消费者无法接收到消息，只有当生产者的本地事物提交后消费者才能看到消息接着消费。
本地事物：提交半消息后执行。结果有3种COMMIT、ROLLBACK、UNKNOW。 COMMIT的话正常走流程，生产者流程结束，消费者拿到消息执行消费逻辑。ROLLBACK的话不再发送消息。UNKONW的话会执行checkLocalTransaction回查本地事物的结果，再按照上述2个状态走之后流程，最多回查15次。
消息清理：针对rollback消息，mq不会做物理层面的删除。mq通过利用op消息来标记消息的最终态，rollback状态的消息不会创建消费队列索引，在消息消费的时候会过滤掉rollback类型的消息从而实现，消息的废弃。


消费者

负载均衡
mq才用的是推拉结合的模式
1.broker推送时机
● 当有consumer加入group
● consumer下线
● broker中queue发生变化
2.consumer拉取时机
● 采用异步线程，每隔20s向broker向broker发送rebalance消息，broker会返回此时在线的group consumer信息，queue信息。
push模式本质上也是用pull模式实现的，pull是主动调用api实现。consumer启动的时候会向broker集群创建长链接并注册consumer实例，broker感知到有新的consumer加入后，会给consumer group发送rebalance请求，consumer group会重新分配queue，当consumer 获取到queue的时候会首次触发从consumer拉取的动作，流程启动后会源源不断的拉取信息。-----所以rokcetmq其实是推拉的变体（长链接获取信息）。
consumer 获取不到信息怎么办，会有定时任务注册每3秒重新拉取一次。
所以您说得对，RocketMQ 的 Push 模式本质上是：
● 基于长连接的状态维护
● 基于长轮询的消息拉取
● 通过内部的拉取服务实现自动循环拉取
● 对用户屏蔽了拉取细节，表现为推送的形式
这种设计的优点：
● 兼顾实时性和性能
● 服务端可以控制流量
● 客户端可以控制消费速度
● 网络资源利用更合理

支持算法
1.平均分配
2.环形分配
3.一致性哈希
4.机房优先
负载均衡策略				说明	适用场景	优缺点
AllocateMessageQueueAveragely （平均分配）		按顺序，均匀地分配队列		Consumer 数量稳定，任务负载均衡		✅ 分配均匀，适合 Consumer 数量稳定 的情况❌ Consumer 上下线会导致队列重新分配，影响性能
AllocateMessageQueueAveragelyByCircle （环形分配）			轮流分配 队列，使得每个 Consumer 处理的队列分散		Consumer 可能频繁上下线，需要降低影响	✅ 适合 Consumer 频繁变动的情况，减少 Rebalance 影响❌ 可能导致一个 Consumer 处理多个不同 Broker 的队列，增加跨 Broker 交互
AllocateMessageQueueConsistentHash （一致性哈希分配）			基于哈希 计算，尽可能让相同的 Consumer 处理相同的队列	需要确保相同 Consumer 处理相同类型的任务（如分片任务）	✅ Consumer 变更影响小，减少 Rebalance❌ 分配不均衡，某些 Consumer 可能负载过重
AllocateMessageQueueByConfig （手动配置分配）		手动指定 Consumer 需要消费哪些队列	特定业务需求，如部分队列需要由特定 Consumer 处理		✅ 完全自定义，可以控制消费逻辑❌ 需要额外配置，适用于 固定场景

总结：RocketMQ consumer rebalance很有意思，broker充当一个协调者，并没真正为consumer分配consumer queue，这是由于每个topic可以多broker部署，broker彼此之间感知不到consumer queue信息，所以broker无法承担rebalance职责，但NameServer其实能拿到必要信息为consumer rebalance consumer queue的，但可能会为NameServer带来更多职责，违背了设计初衷。consumer group 内的consumer遍历所有broker获取到每个topic下的queue、consumer数量和详情，然后每个consumer使用相同的数据，相同的算法，计算得到相同consumer queue结果。由于缺少统一协调者，在rebalance过程会存在一致性差异，可能同一个consumer queue在短时间内被多个consumer消费，因此消费端一定要做好幂等。

消费进度管理
消费位点是如何维护的？
集群模式下消费位点是放在broker维护的，broker中offsetTable是一个双层Map <topic, <queueId, offset>>，每次集群中有消费者上报消费位点时会更新消费进度，为了防止broker宕机丢失消费进度，broker有定时任务，每间隔10s将消费进度同步至机器上的json文件中，待机器重启后根据json文件获取集群消费进度。
消费者从broker拉取消息时，返回的结构体会包含下面几个信息，拉取到的消息就不用说了，nextBeginOffset指明了下次拉取消息的起始地址。
● 它会保存拉取到的信息msgs
● 逻辑消费队列的nextBeginOffset,这个参数非常非常的重要，它就表示消费者下次消费时从哪开始读取消息(指的是消息索引，根据索引读取真正的消息）。
● 逻辑消费队列的 minOffset，最小消费偏移量
● 逻辑消费队列的 maxOffset, 最大消费偏移量
有个问题需要思考下，消费者从broker拉取消息时是否要使用offsetTable根据消费位点为consumer发放消息？其实不需要，因为nextBeginOffset字段的存在，服务端只需要根据nextBeginOffset+batchSize就可以确定好下一个批次哪些消息需要发放给consumer，既然offset不是消费时的必要信息，那也没有必要在每次消费结束后consumer实时向broker上报已经消费的offset位点，提升系统性能，因此，每个consumer本地也维护了个offsetTable，结构格式和broker中offsetTable差不多，定时异步的向broker上报当前consumer 各个Topic queue的消费位点。
主从模式下如何维护消费位点?
为了保证RocketMQ broker的可用性，通常会对broker按照主从模式进行集群部署，通常情况下master节点接收写入请求，consumer也是从master读取消息消费，但是如果master资源压力较大时，会在从slave节点读取数据消费，主从节点均可消费的情况下，必然会存在数据一致性问题，主从模式如何维护消费位点？如何保证消息不丢失？这其实要要依赖nextBeginOffset字段，每次消费时都会根据nextBeginOffset字段选择下一个批次要消费的消息，理论上用不到offsetTable消费位点中的信息，只有当新的consumer加入是才需要根据offsetTable选择第一个批次起始消费位点，其余情况下不需要考虑，而consumer消费位点是通过异步定时任务和master节点交互更新offsetTable的，而slave节点会定时从master节点同步最新的消费位点。一旦master宕机，某个slave节点会被选举成主节点，如果master存在消费位点没有同步至slave，当有新的consumer加入时会有一段时间数据（10s slavle同步一次消费位点）的重复消费，业务需要保证消息消费幂等，不会产生什么负面影响。
关键点
1. ConcurrentMap<String/* group */, ConcurrentMap<String/* topic@queueId */, Long>> offsetTable; 是全局的消费进度表
2. nextBeginOffset 是当前处理队列的进度指针  是内存上一个逻辑存在的进度且这个字段是 每个consumer 私有的
3. offsetTable 需要持久化，nextBeginOffset 不需要
4. 两者需要保持同步更新
5. offsetTable 用于重启恢复，nextBeginOffset 用于实时拉取

思考
● 简单：以pull模式实现push模式，如果通过broker实现主动推的能力，会给broker 服务端带来极大压力，同时会增大broker服务单复杂度，通过改造客户端，将服务端逻辑上移至客户端中，可以极大降低系统复杂度。如果某些能力broker服务端和Producer、consumer客户端实现起来都非常困难，复杂度极高，可以考虑将这种功能继续上移，让接入业务方自己实现，要清晰识别出哪些应该由平台维护，哪些应该是业务实现。例如RocketMQ中幂等实现，如果依赖RocketMQ保证exactly once会带来极高复杂度，因此将这部分职责上移，交由业务实现。
● 全局设计：nextBeginOffset字段的设计，依赖nextBeginOffset将offset消费位点和消息消费解耦开，offset作用只是在consumer初次加入时作为endpoint，作为消息初始拉取位点，在consumer正常运行时通过nextBeginOffset拉取下一个消息批次，由于这个字段的存在，在ha模式下和RocketMQ consumer业务保证幂等设计相结合，consumer可以自由的选择在master和slave节点消费消息，最大影响是有一小段时间的消息重复消费，提高RocketMQ的高可用。


RocketMQ和kafka对比
可靠性
● RocketMQ：支持异步实时刷盘、同步刷盘、同步复制、异步复制。
● kafka：使用异步刷盘方式，异步复制/同步复制。
总结：
1. RocketMQ支持kafka所不具备的“同步刷盘”功能，在单机可靠性上比kafka更高，不会因为操作系统Crash而导致数据丢失。
2. kafka的同步replication理论上性能低于RocketMQ的replication，这是因为kafka的数据以partition为单位，这样一个kafka实例上可能多上百个partition。而一个RocketMQ实例上只有一个partition，RocketMQ可以充分利用IO组的commit机制，批量传输数据。同步replication与异步replication相比，同步replication性能上损耗约20%-30%。
   一句话概括：RocketMQ新增了同步刷盘机制，保证了可靠性；一个RocketMQ实例只有一个partition, 在replication时性能更好。
   性能比对
1. kafka单机写入TPS月在百万条/秒，消息大小为10个字节。
2. RocketMQ单机写入TPS单实例约7万条/秒，若单机部署3个broker，可以跑到最高12万条/秒，消息大小为10个字节。
   总结：
   ● kafka的单机TPS能跑到每秒上百万，是因为Producer端将多个小消息合并，批量发向broker。
   那么RocketMQ为什么没有这样做呢？
   ● 发送消息的Producer通常是用Java语言，缓存过多消息，GC是个很严重的问题。（问题：难道kafka用scala不需要GC？）
   ● Producer发送消息到broker, 若消息发送出去后，未达到broker，就通知业务消息发送成功，若此时Broker宕机，则会导致消息丢失，从而导致业务出错。
   ● Producer通常为分布式系统，且每台机器都是多线程发送，通常来说线上单Producer产生的消息数量不会过万。消息合并功能完全可由上层业务来做。
   一句话概括：RocketMQ写入性能上不如kafka, 主要因为kafka主要应用于日志场景，而RocketMQ应用于业务场景，为了保证消息必达牺牲了性能，且基于线上真实场景没有在RocketMQ层做消息合并，推荐在业务层自己做。
   消息有序
   一句话概括：kafka不保证消息有序，RocketMQ可保证严格的消息顺序，即使单台Broker宕机，仅会造成消息发送失败，但不会消息乱序。

定时消息
kafka不支持定时消息
rocketmq支持定时消息，通过设置延迟级别，无法自定义时间，实际场景下更多使用hts实现定时任务
// RocketMQ 支持的延迟级别
private static final int[] DELAY_LEVELS = {
1,    // 1s
5,    // 5s
10,   // 10s
30,   // 30s
60,   // 1min
120,  // 2min
180,  // 3min
240,  // 4min
300,  // 5min
360,  // 6min
420,  // 7min
480,  // 8min
540,  // 9min
600,  // 10min
1200, // 20min
1800, // 30min
3600  // 1h
};
分布式任务
1. kafka不支持分布式事务消息
2. RocketMQ支持分布式事务消息。
   消息查询
1. kafka不支持消息查询
2. RocketMQ支持根据消息标识（发送消息时指定一个消息key, 任意字符串，如指定为订单编号）查询消息，也支持根据消息内容查询消息。
   总结：消息查询功能对于定位消息丢失问题非常有用，例如某个订单处理失败，可用此功能查询是消息没收到，还是收到了但处理出错了。
   消息回溯
1. kafka可按照消息的offset来回溯消息
2. RocketMQ支持offset回溯，同时也支持按照时间来回溯消息，精度到毫秒，例如从一天的几点几分几秒几毫秒来重新消费消息。
   总结：RocketMQ按时间做回溯消息的典型应用场景为，consumer做订单分析，但是由于程序逻辑或依赖的系统发生故障等原因，导致今vs天处理的消息全部无效，需要从昨天的零点重新处理。
   架构不同
   RocketMQ是一主多从模式
   kafka是多主多从，以partition为部署单位，每个topic有多个partition，由于多个partition彼此不感知所以kafka是不保证全局有序的



刷盘代码
刷盘的主要代码在commitLog中submitFlushRequest。
真正执行刷盘逻辑的代码在GroupCommitService，该service顶层实现了runnable接口，
实际的调用链是
BrokerController.start()
-> DefaultMessageStore.start()
-> CommitLog.start()
-> GroupCommitService.start()
public CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result, MessageExt messageExt) {
// Synchronization flush
if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
if (messageExt.isWaitStoreMsgOK()) {
GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
flushDiskWatcher.add(request);
service.putRequest(request);
return request.future();
} else {
service.wakeup();
return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
}
}
// Asynchronous flush
else {
if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
flushCommitLogService.wakeup();
} else  {
commitLogService.wakeup();
}
return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
}
}
public synchronized void putRequest(final GroupCommitRequest request) {
lock.lock();
try {
this.requestsWrite.add(request);
} finally {
lock.unlock();
}
// 有任务提交的时候被立即唤醒，和run 方法中的waitForRunning对应
this.wakeup();
}

public void run() {
CommitLog.log.info(this.getServiceName() + " service started");
// 经典空循环
while (!this.isStopped()) {
try {
// 受控的空循环，最多等待10ms，因为当有任务提交的时候会被立即唤醒 putRequest
this.waitForRunning(10);
// 执行刷盘
this.doCommit();
} catch (Exception e) {
CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
}
}

    // Under normal circumstances shutdown, wait for the arrival of the
    // request, and then flush
    try {
        // 同步刷盘会等10ms
        Thread.sleep(10);
    } catch (InterruptedException e) {
        CommitLog.log.warn(this.getServiceName() + " Exception, ", e);
    }
    // 交换 读写链表 实现读写分离降低 多线程冲突 提高性能
    synchronized (this) {
        this.swapRequests();
    }
    // 确保所有请求被处理完
    this.doCommit();

    CommitLog.log.info(this.getServiceName() + " service end");
}
private void doCommit() {
if (!this.requestsRead.isEmpty()) {
for (GroupCommitRequest req : this.requestsRead) {
// There may be a message in the next file, so a maximum of
// two times the flush
boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
for (int i = 0; i < 2 && !flushOK; i++) {
CommitLog.this.mappedFileQueue.flush(0);
flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
}

            req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
        }

        long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
        if (storeTimestamp > 0) {
            CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
        }

        this.requestsRead = new LinkedList<>();
    } else {
        // Because of individual messages is set to not sync flush, it
        // will come to this process
        CommitLog.this.mappedFileQueue.flush(0);
    }
}
异步刷盘有2种
1. 未开启暂存池 flushCommitLogService
   @Override
   public void run() {
   CommitLog.log.info(this.getServiceName() + " service started");
   while (!this.isStopped()) {
   int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

        int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

        int commitDataThoroughInterval =
            CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

        long begin = System.currentTimeMillis();
        //如果当前时间大于上次提交时间+两次提交的最大间隔时间，意味着已经有比较长的一段时间没有进行提交了，需要尽快刷盘，此时将每次提交的最少页数设置为0不限制提交页数
        if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
            this.lastCommitTimestamp = begin;
            commitDataLeastPages = 0;
        }

        try {
            boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
            long end = System.currentTimeMillis();
            // 返回false 表示有数据提交 需要刷盘 这个设计比较反直觉
            if (!result) {
                this.lastCommitTimestamp = end; // result = false means some data committed.
                //now wake up flush thread.
                //如果还有数据需要提交，唤醒刷盘线程
                flushCommitLogService.wakeup();
            }

            if (end - begin > 500) {
                log.info("Commit data to file costs {} ms", end - begin);
            }
            this.waitForRunning(interval);
        } catch (Throwable e) {
            CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
        }
   }

   boolean result = false;
   // 最多重试RETRY_TIMES_OVER次，确保数据都提交成功
   for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
   result = CommitLog.this.mappedFileQueue.commit(0);
   CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
   }
   CommitLog.log.info(this.getServiceName() + " service end");
   }
   }
   public boolean commit(final int commitLeastPages) {
   boolean result = true;
   // 找到当前提交位置对应的MappedFile
   MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
   if (mappedFile != null) {
   // 提交数据到MappedFile
   int offset = mappedFile.commit(commitLeastPages);
   // 更新提交位置
   long where = mappedFile.getFileFromOffset() + offset;
   // 设置返回结果，如果本次提交偏移量等于上一次的提交偏移量为true，表示什么也没干，否则表示提交了数据，等待刷盘
   result = where == this.committedWhere;
   // 更新当前提交位置
   this.committedWhere = where;
   }

   return result;
   }
   public int commit(final int commitLeastPages) {
   // 如果writeBuffer为空，表示没有数据需要提交，直接返回wrotePosition
   if (writeBuffer == null) {
   //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
   return this.wrotePosition.get();
   }
   // 如果可以提交数据
   if (this.isAbleToCommit(commitLeastPages)) {
   if (this.hold()) {
   commit0();
   this.release();
   } else {
   log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
   }
   }

   // All dirty data has been committed to FileChannel.
   if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
   this.transientStorePool.returnBuffer(writeBuffer);
   this.writeBuffer = null;
   }

   return this.committedPosition.get();
   }
   protected boolean isAbleToCommit(final int commitLeastPages) {
   // 获取提交数据的位置偏移量
   int flush = this.committedPosition.get();
   // 获取已写入位置
   int write = this.wrotePosition.get();
   // 如果文件已满，直接返回true
   if (this.isFull()) {
   return true;
   }
   // 如果提交数据的最少页数大于0，表示需要提交数据
   if (commitLeastPages > 0) {
   // 如果已写入位置减去提交位置大于等于提交数据的最少页数，表示可以提交数据
   return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
   }
   // 如果已写入位置大于提交位置，表示可以提交数据
   return write > flush;
   }
   protected void commit0() {
   // 获取已写入位置       
   int writePos = this.wrotePosition.get();
   // 获取上次提交位置
   int lastCommittedPosition = this.committedPosition.get();
   // 如果已写入位置大于上次提交位置，表示有数据需要提交
   if (writePos - lastCommittedPosition > 0) {
   try {
   // 获取写缓冲区
   ByteBuffer byteBuffer = writeBuffer.slice();
   // 设置写缓冲区位置
   byteBuffer.position(lastCommittedPosition);
   // 设置写缓冲区限制
   byteBuffer.limit(writePos);
   // 设置文件通道位置
   this.fileChannel.position(lastCommittedPosition);
   // 写入数据
   this.fileChannel.write(byteBuffer);
   // 设置提交位置
   this.committedPosition.set(writePos);
   } catch (Throwable e) {
   log.error("Error occurred when commit data to FileChannel.", e);
   }
   }
   }

2. 开启暂存池 FlushRealTimeService
