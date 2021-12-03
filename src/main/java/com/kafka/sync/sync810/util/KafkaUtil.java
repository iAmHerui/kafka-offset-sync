package com.kafka.sync.sync810.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Properties;

public class KafkaUtil {
    /**
     * 实例化消费者组
     *
     * @param bootstrapServers
     * @param consumerGroupId
     * @return
     */
    public static KafkaConsumer getConsumerInstance(String bootstrapServers, String consumerGroupId, String autoCommit) {
        Properties consProps = new Properties();
        //bootstrap.servers 目的集群地址
        consProps.put("bootstrap.servers", bootstrapServers);
        consProps.put("group.id", consumerGroupId);
        consProps.put("enable.auto.commit", autoCommit);
        consProps.put("auto.offset.reset", "earliest");
        consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps.put("value.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");

        return new KafkaConsumer<>(consProps);
    }
    public static KafkaConsumer getConsumerInstance(String bootstrapServers, String consumerGroupId) {

        return getConsumerInstance(bootstrapServers, consumerGroupId, "false");
    }

    /**
     * 实例化生产者
     *
     * @param bootstrapServers
     * @return
     */
    public static KafkaProducer<String, Bytes> getProducerInstance(String bootstrapServers) {
        Properties proProps = new Properties();
        proProps.put("bootstrap.servers", bootstrapServers);
        proProps.put("acks", "1");
        proProps.put("retries", 1);
        proProps.put("batch.size", 16384);
        proProps.put("linger.ms", 10);
        proProps.put("buffer.memory", 33554432);
        proProps.put("metadata.max.age.ms", 10000);
        proProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        proProps.put("value.serializer", "org.apache.kafka.common.serialization.BytesSerializer");

        return new KafkaProducer<>(proProps);
    }

    /**
     * 实例化 adminClient
     *
     * @param bootstrapServers
     * @return
     */
    public static AdminClient getAdminClientInstance(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        return AdminClient.create(props);
    }

    /**
     * 关闭kafka consumer连接
     *
     * @param consumer
     */
    public static void closeConsumer(KafkaConsumer consumer) {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 关闭kafka producer
     *
     * @param producer
     */
    public static void closeProducer(KafkaProducer producer) {
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭kafka adminClient
     *
     * @param adminClient
     */
    public static void closeAdminClient(AdminClient adminClient) {
        try {
            if (adminClient != null) {
                adminClient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
