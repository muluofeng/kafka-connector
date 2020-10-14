package com.example.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * @author xiexingxing
 * @Created by 2020-10-13 14:54.
 */
@Slf4j
@Configuration
public class KafkaConfig implements InitializingBean {


    public List<String> topics = new ArrayList<>();

    @Override
    public void afterPropertiesSet()  {
        topics.add("test.mybatis-demo.users");
        topics.add("test.mybatis-demo.orders");
        startTopicListener();
    }
    /**
     * 手动创建批量消息监听器
     * @param topic
     * @return
     */
    public KafkaMessageListenerContainer manualListenerContainer(String topic) {
        ContainerProperties properties = new ContainerProperties(topic);
        properties.setGroupId("dd");
//        properties.setAckMode(AbstractMessageListenerContainer.AckMode.BATCH);

        properties.setMessageListener((new BatchAcknowledgingMessageListener<String, String>() {
            @Override
            public void onMessage(List<ConsumerRecord<String, String>> list, Acknowledgment acknowledgment) {
                long startTime = System.currentTimeMillis();
                log.info("本次批量获取线程ID:{},消息长度:{}",Thread.currentThread().getId(),list.size());
//                batchMsgHandler(list, acknowledgment);
                log.info("本次批量消费耗时{}毫秒,",System.currentTimeMillis()-startTime);
            }
        }));
        KafkaMessageListenerContainer container = new KafkaMessageListenerContainer(consumerFactory(), properties);
        return container;
    }


    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"106.54.237.135:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //put all configs in map
        return props;
    }
    /**
     * 启动时默认加载已经存在的表对应的topic
     */
    public void startTopicListener(){
        List<KafkaMessageListenerContainer> listeners = new ArrayList<>();
        for (String topic : topics) {
            KafkaMessageListenerContainer container =  manualListenerContainer(topic);
            listeners.add(container);
        }
        for (KafkaMessageListenerContainer listener : listeners) {
            try {
                listener.start();
            }catch (Exception e){
                log.error("监听器启动失败,异常:{}",e.getMessage(),e);
            }
        }
    }




}