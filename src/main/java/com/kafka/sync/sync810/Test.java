package com.kafka.sync.sync810;

import com.kafka.sync.sync810.entity.CustomOffsetAndMetada;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Test {

    public static void main(String[] args) {

        //------------容灾切换------------
        sourceToTargetCommmitOffsetTest();

    }

    /**
     * 容灾切换，将消费位移提交
     */
    public static void sourceToTargetCommmitOffsetTest() {
        // ISV 服务定义的消费者者组
        String consumerGroupId = "group02";
        // 目的集群地址
        String bootstrapServers = "10.121.91.143:6667,10.121.91.137:6667,10.121.91.141:6667";
        // ISV 服务消费者组订阅的topic
        List<String> topics = Collections.singletonList("test001");
        // 源端kafka别名
        String remoteClusterAlias = "kafka2";

        KafkaOffsetCommit kafkaOffsetCommit = new KafkaOffsetCommit();
        // 获取ISV消费者组消费的位移
        Map<TopicPartition, CustomOffsetAndMetada> metadataMap = kafkaOffsetCommit.remoteConsumerOffsets(consumerGroupId, remoteClusterAlias, bootstrapServers, topics);
        //将用于消息转换的消费者组的消费位移提交到相应位置
        kafkaOffsetCommit.commitOffset(metadataMap, bootstrapServers, consumerGroupId);
    }

}
