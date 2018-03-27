package com.cetc.kafkademo.kafka;

import com.cetc.kafkademo.Mapper.TestMapper;
import com.cetc.kafkademo.Model.Student;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by FangYan on 2018/3/14.
 */
@Component
@Slf4j
public class Consumer {



    @Autowired private ObjectMapper objectMapper;

    @Autowired private TestMapper testMapper;

    @Autowired private KafkaTemplate kafkaTemplate;

    private RuntimeSchema<Student> schema = RuntimeSchema.createFrom(Student.class);


        //消费消息，简单的一条一条的消费，如果需要批量消费，需要自己配置
    @KafkaListener(topics = "student")
    public void handleMessage(byte[] content) throws InterruptedException {  //数组也是对象
        Thread.sleep(1000);
        Student student = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(content,student, schema);
        System.out.println("我是消费者"+"名字："+ student.getName()+"   年龄："+student.getAge());
//        testMapper.addStudent(student);
        kafkaTemplate.send("student",content);
    }

//    //接受多条数据
//    @KafkaListener(id = "list", topics = "student", containerFactory = "batchFactory")
//    public void listen(List<String> list){
//        Student student = schema.newMessage();
//        ProtostuffIOUtil.mergeFrom(list.get(0).getBytes(),student, schema);
//        System.out.println("我是消费者"+"名字："+ student.getName()+"   年龄："+student.getAge());
//        testMapper.addStudent(student);
//    }


//    @Bean
    public KafkaListenerContainerFactory<?> batchFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        return factory;
    }


    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.104:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);//每一批数量
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        return props;
    }

    /**
     * earliest当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
     *
     * latest当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
     *
     * none topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */

//    @Bean
    public ConsumerFactory consumerFactory() {
        return new DefaultKafkaConsumerFactory(consumerConfigs());
    }
}

