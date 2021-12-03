package com.kafka.sync.sync810;

import com.kafka.sync.sync810.entity.CustomOffsetAndMetada;
import com.kafka.sync.sync810.util.KafkaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.mirror.Checkpoint;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaOffsetCommit {


    /**
     * Translate a remote consumer group's offsets into corresponding local offsets. Topics are automatically
     * renamed according to the ReplicationPolicy.
     *
     * @param consumerGroupId    要查询的消费者组id
     * @param remoteClusterAlias 源端kafka别名
     * @param bootstrapServers   目的端kafka地址
     * @param topics             要查询的消费者组订阅的topic名称
     * @return
     */
    public Map<TopicPartition, CustomOffsetAndMetada> remoteConsumerOffsets(String consumerGroupId,
                                                                            String remoteClusterAlias, String bootstrapServers, List<String> topics) {
        Map<TopicPartition, CustomOffsetAndMetada> offsets = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("consumer.client.id", "mm2-clinet2");
        properties.put("consumer.enable.auto.commit", "true");
        properties.put("consumer.auto.offset.reset", "earliest");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties,
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        try {
            // checkpoint topics are not "remote topics", as they are not replicated. So we don't need
            // to use ReplicationPolicy to create the checkpoint topic here.
            String CHECKPOINTS_TOPIC_SUFFIX = ".checkpoints.internal";
            String checkpointTopic = remoteClusterAlias + CHECKPOINTS_TOPIC_SUFFIX;
            List<TopicPartition> checkpointAssignment =
                    Collections.singletonList(new TopicPartition(checkpointTopic, 0));

            //获取topic的分区信息，用于校验group消费位移的完整性
            Set<TopicPartition> assignments = new HashSet<>();
            for (String topic : topics) {
                Set<TopicPartition> assignment = consumer.partitionsFor(remoteClusterAlias + "." + topic)
                        .stream().map(t -> new TopicPartition(t.topic(), t.partition())).collect(Collectors.toSet());
                assignments.addAll(assignment);
            }

            //获取checkpointTopic 的offset的结尾和开始处
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(checkpointAssignment);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(checkpointAssignment);
            TopicPartition checkpointTopicPartition = new TopicPartition(checkpointTopic, 0);
            long beginningOffset = beginningOffsets.get(checkpointTopicPartition);
            long commitOffset = endOffsets.get(checkpointTopicPartition) - 1024L;
            commitOffset = commitOffset >= beginningOffset ? commitOffset : beginningOffset;
            //为消费者分配partition，重置消费位移到endOffset-1024处
            consumer.assign(checkpointAssignment);
            consumer.seek(checkpointTopicPartition, commitOffset);

            // 判断获取的消费者组的消费位移信息是否完整，不完整继续消费
            while (!informationIntegrity(consumer, offsets, checkpointTopicPartition, assignments, endOffsets, beginningOffsets)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        Checkpoint checkpoint = Checkpoint.deserializeRecord(record);
                        if (checkpoint.consumerGroupId().equals(consumerGroupId)) {
                            CustomOffsetAndMetada metadata = offsets.get(checkpoint.topicPartition());
                            OffsetAndMetadata offsetAndMetadata = checkpoint.offsetAndMetadata();
                            CustomOffsetAndMetada customOffsetAndMetada = new CustomOffsetAndMetada(offsetAndMetadata.offset(), offsetAndMetadata.metadata(), record.timestamp());
                            //过了掉非指定topic
                            if (!assignments.contains(checkpoint.topicPartition())) {
                                continue;
                            }
                            if (metadata == null) {
                                offsets.put(checkpoint.topicPartition(), customOffsetAndMetada);
                            } else {
                                if (record.timestamp() >= metadata.getTimestamp()) {
                                    offsets.put(checkpoint.topicPartition(), customOffsetAndMetada);
                                }
                            }
                        }
                    } catch (SchemaException e) {
                        System.out.println("Could not deserialize record. Skipping.");
                        e.getStackTrace();
                    }
                }
            }
            System.out.println("Consumed " + offsets.size() + " checkpoint records for " + consumerGroupId + " from " + checkpointTopic + ".");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        } finally {
            KafkaUtil.closeConsumer(consumer);
        }
        return offsets;
    }

    /**
     * 检查是否已经获取消费者组的全部位移信息
     *
     * @param consumer                 消费者
     * @param offsets                  消费者组的位移信息
     * @param checkpointTopicPartition 存储消费者位移信息的topic
     * @param assignments              消费者组订阅的topic的信息
     * @param endOffsets               存储消费者位移信息的topic的截止位置
     * @param beginningOffsets         存储消费者位移信息的topic的开始位置
     * @return
     */
    public boolean informationIntegrity(Consumer<?, ?> consumer, Map<TopicPartition, CustomOffsetAndMetada> offsets, TopicPartition checkpointTopicPartition,
                                        Collection<TopicPartition> assignments, Map<TopicPartition, Long> endOffsets, Map<TopicPartition, Long> beginningOffsets) {
        // 判断是否消费到截止位置
        Long endOffset = endOffsets.get(checkpointTopicPartition);
        boolean endOfStream = consumer.position(checkpointTopicPartition) < endOffset ? false : true;
        // 如果已经从定位位置消费到结束位置，判断获取的消费者组信息是否完整
        if (endOfStream) {
            //获取消费者组的消费位移的Partition，和订阅组的Partition，比较，判断费者组的消费位移信息是否完整
            Set<TopicPartition> groupTopicPartitionSet = new HashSet<>();
            groupTopicPartitionSet.addAll(offsets.keySet());
            int groupTopicPartitionCount = groupTopicPartitionSet.size();
            groupTopicPartitionSet.addAll(assignments);
            if (groupTopicPartitionSet.size() > groupTopicPartitionCount) {
                // 消费者组位移信息不完整
                // 判断是否已经是从checkpointTopicPartition的开始位置消费的，如果是，checkpointTopicPartition 已经从头到尾消费完成，不管信息是否完整均需返回
                long offset = endOffset - 1024L;
                if (offset <= beginningOffsets.get(checkpointTopicPartition)) {
                    return true;
                }
                // checkpointTopicPartition还未消费完，重新定位消费位置
                // 已经消费到截止位置，重新定义截止位置，重新seek消费位置, 返回结果false，信息不完整，继续向前消费1024条信息
                endOffsets.put(checkpointTopicPartition, offset);
                long beginningOffset = beginningOffsets.get(checkpointTopicPartition);
                long commitOffset = offset - 1024L;
                commitOffset = commitOffset >= beginningOffset ? commitOffset : beginningOffset;
                consumer.seek(checkpointTopicPartition, commitOffset);
                return false;
            } else {
                // 消费者组位移信息完整
                return true;
            }
        }
        // 未消费到截止位置
        return false;
    }


    /**
     * 提交位移,将用于A.topic1转移到topic1的内部消费者组的消费位移提交到相应位置
     *
     * @param metadataMap      消费者组位移信息
     * @param bootstrapServers 目的端地址
     * @param consumerGroupId  消费者组
     */
    public boolean commitOffset(Map<TopicPartition, CustomOffsetAndMetada> metadataMap, String bootstrapServers, String consumerGroupId) {
        boolean result = true;
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = KafkaUtil.getConsumerInstance(bootstrapServers, consumerGroupId, "true");
            // 获取目的集群topic的分区信息，为consumer分配Partition，
            Set<TopicPartition> assignments = metadataMap.keySet();
            consumer.assign(assignments);

            // 位移提交
            Set<TopicPartition> topicPartitions = metadataMap.keySet();
            Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(topicPartitions);
            for (TopicPartition topicPartition : topicPartitions) {
                OffsetAndMetadata metadata = metadataMap.get(topicPartition);
                Long topicEndOffset = topicPartitionLongMap.get(topicPartition);
                long offset = metadata.offset() >= topicEndOffset ? topicEndOffset : metadata.offset();
                consumer.seek(topicPartition, offset);
            }
        } catch (Exception e) {
            System.out.println("提交位移失败");
            e.printStackTrace();
            result = false;
        } finally {
            KafkaUtil.closeConsumer(consumer);
        }
        return result;
    }

}
