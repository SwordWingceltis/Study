随着业务量的急速膨胀，我们会对现有的Kafka集群进行扩容，以应对更大的流量和业务尖峰。当然，扩容之后新的Kafka Broker默认是不会有任何Topic和Partition的，需要手动利用分区重分配命令kafka-reassign-partitions将现有的Partition/Replica平衡到新的Broker上去。那么Kafka具体是如何执行重分配流程的呢？
###生产、提交重分配方案
我们知道，使用kafka-reassign-partitions命令分为三步，一是根据指定的Topic生成JSON格式的重分配方案(--generate)，二是将生成的方案提交执行(--execute)，三是观察重分配的进度(--verify)。它们分别对应kafka.admin.ReassignPartitionsCommand类中的generateAssignmen()、executeAssignment()和verifyAssignment()方法。

generateAssignmen()方法会调用AdminUtils#assignReplicasToBrokers()方法生成Replica分配方案。其原则简述如下：

- 将Replica尽量均匀地分配到各个Broker上去；
- 一个Partition的所有Replica必须位于不同的Broker上；
- 如果Broker有机架感知（rack aware）的信息，将Partition的Replica尽量分配到不同的机架。

executeReassignment()方法调用了reassignPartitions()方法，其源码如下。
```
def reassignPartitions(throttle: Throttle = NoThrottle, timeoutMs: Long = 10000L): Boolean = {
     maybeThrottle(throttle)
     try {
       val validPartitions = proposedPartitionAssignment.filter { case (p, _) => validatePartition(zkUtils, p.topic, p.partition) }
       if (validPartitions.isEmpty) false
       else {
         if (proposedReplicaAssignment.nonEmpty) {
           val adminClient = adminClientOpt.getOrElse(
             throw new AdminCommandFailedException("bootstrap-server needs to be provided in order to reassign replica to the specified log directory"))
           val alterReplicaDirResult = adminClient.alterReplicaLogDirs(
             proposedReplicaAssignment.asJava, new AlterReplicaLogDirsOptions().timeoutMs(timeoutMs.toInt))
           alterReplicaDirResult.values().asScala.foreach { case (replica, future) => {
               try {
                 future.get()
                 throw new AdminCommandFailedException(s"Partition ${replica.topic()}-${replica.partition()} already exists on broker ${replica.brokerId()}." +
                   s" Reassign replica to another log directory on the same broker is currently not supported.")
               } catch {
                 case t: ExecutionException =>
                   t.getCause match {
                     case e: ReplicaNotAvailableException => // It is OK if the replica is not available
                     case e: Throwable => throw e
                   }
               }
           }}
         }
         val jsonReassignmentData = ZkUtils.formatAsReassignmentJson(validPartitions)
         zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
         true
       }
     } catch {
       // ......
     }
   }
```
在进行必要的Partition校验之后，创建ZK持久节点/admin/reassign_partitions，并将JSON格式的重分配方案写进去。如果该节点存在，就表示已经在进行重分配，不能再启动新的重分配流程（相关的判断在executeReassignment()方法中）。
###监听并处理重分配事件
在之前讲解Kafka Controller时，提到Controller会注册多个ZK监听器，将监听到的事件投递到内部的事件队列，并由事件处理线程负责处理。监听ZK中/admin/reassign_partitions节点的监听器为PartitionReassignmentListener，并产生PartitionReassignment事件，处理逻辑如下。
```
case object PartitionReassignment extends ControllerEvent {
     def state = ControllerState.PartitionReassignment
   
     override def process(): Unit = {
       if (!isActive) return
       val partitionReassignment = zkUtils.getPartitionsBeingReassigned()
       val partitionsToBeReassigned = partitionReassignment.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
       partitionsToBeReassigned.foreach { case (partition, context) =>
         if(topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)) {
           error(s"Skipping reassignment of partition $partition since it is currently being deleted")
           removePartitionFromReassignedPartitions(partition)
         } else {
           initiateReassignReplicasForTopicPartition(partition, context)
         }
       }
     }
   }
```
该方法先取得需要重分配的Partition列表，然后从中剔除掉那些已经被标记为删除的Topic所属的Partition，再调用initiateReassignReplicasForTopicPartition()方法：
```
def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                                 reassignedPartitionContext: ReassignedPartitionsContext) {
     val newReplicas = reassignedPartitionContext.newReplicas
     val topic = topicAndPartition.topic
     val partition = topicAndPartition.partition
     try {
       val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
       assignedReplicasOpt match {
         case Some(assignedReplicas) =>
           if (assignedReplicas == newReplicas) {
             throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
               " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
           } else {
             info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
             // first register ISR change listener
             watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
             controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
             // mark topic ineligible for deletion for the partitions being reassigned
             topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
             onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
           }
         case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
           .format(topicAndPartition))
       }
     } catch {
       case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
       // remove the partition from the admin path to unblock the admin client
       removePartitionFromReassignedPartitions(topicAndPartition)
     }
   }
```
该方法的执行逻辑如下：
- 判断Partition的原有Replica是否与即将重分配的新Replica相同，如果相同则抛出异常；
- 注册即将被重分配的Partition的ISR变化监听器；
- 把即将被重分配的Partition/Replica记录在Controller上下文中的partitionsBeingReassigned集合中；
- 把即将被重分配的Topic标记为不可删除；
- 调用onPartitionReassignment()方法真正触发重分配流程。
###执行重分配流程
onPartitionReassignment()方法的代码如下。
```
def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
     val reassignedReplicas = reassignedPartitionContext.newReplicas
     if (!areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas)) {
       info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
         "reassigned not yet caught up with the leader")
       val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet
       val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet
       //1. Update AR in ZK with OAR + RAR.
       updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)
       //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
       updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
         newAndOldReplicas.toSeq)
       //3. replicas in RAR - OAR -> NewReplica
       startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
       info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
         "reassigned to catch up with the leader")
     } else {
       //4. Wait until all replicas in RAR are in sync with the leader.
       val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
       //5. replicas in RAR -> OnlineReplica
       reassignedReplicas.foreach { replica =>
         replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
           replica)), OnlineReplica)
       }
       //6. Set AR to RAR in memory.
       //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
       //   a new AR (using RAR) and same isr to every broker in RAR
       moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
       //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
       //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
       stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)
       //10. Update AR in ZK with RAR.
       updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)
       //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
       removePartitionFromReassignedPartitions(topicAndPartition)
       info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
       controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
       //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
       sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
       // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
       topicDeletionManager.resumeDeletionForTopics(Set(topicAndPartition.topic))
     }
   }
```
官方JavaDoc比较详细，给出了3个方便解释流程的定义，列举如下：
- RAR（Re-assigned replicas）：重分配的Replica集合，记为reassignedReplicas；
- OAR（Original assigned replicas）：重分配之前的原始Replica集合，通过controllerContext.partitionReplicaAssignment()方法取得；
- AR（Assigned replicas）：当前的Replica集合，随着重分配的进行不断变化。

根据上文的代码和注释，我们可以很容易地梳理出重分配的具体流程：

(0). 检查RAR是否都已经在Partition的ISR集合中（即是否已经同步），若否，则计算RAR与OAR的差集，也就是需要被创建或者重分配的Replica集合；

(1) 计算RAR和OAR的并集，即所有Replica的集合，并将ZK中的AR更新；

(2) 增加Partition的Leader纪元值，并向AR中的所有Replica所在的Broker发送LeaderAndIsrRequest；

(3) 更新RAR与OAR的差集中Replica的状态为NewReplica，以触发这些Replica的创建或同步；

(4) 计算OAR和RAR的差集，即重分配过程中需要被下线的Replica集合；

(5) 等待RAR都已经在Partition的ISR集合中，将RAR中Replica的状态设置为OnlineReplica，表示同步完成；

(6) 将迁移现场的AR更新为RAR；

(7) 检查Partition的Leader是否在RAR中，如果没有，则触发新的Leader选举。然后增加Partition的Leader纪元值，发送LeaderAndIsrRequest更新Leader的结果；

(8~9) 将OAR和RAR的差集中的Replica状态设为Offline->NonExistentReplica，这些Replica后续将被删除；

(10) 将ZK中的AR集合更新为RAR；

(11) 一个Partition重分配完成，更新/admin/reassign_partitions节点中的执行计划，删掉完成的Partition；

(12) 发送UpdateMetadataRequest给所有Broker，刷新元数据缓存；

(13) 如果有一个Topic已经重分配完成并且将被删除，就将它从不可删除的Topic集合中移除。

最后一个小问题：Partition重分配往往会涉及大量的数据交换，有可能会影响正常业务的运行，如何避免呢？ReassignPartitionsCommand也提供了throttle功能用于限流，在代码和帮助文档中都可以看到它，就不多讲了。当然，一旦启用了throttle，我们一定要定期进行verify操作，防止因为限流导致重分配的Replica一直追不上Leader的情况发生。