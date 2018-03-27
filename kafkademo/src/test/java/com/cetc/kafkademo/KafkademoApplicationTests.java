package com.cetc.kafkademo;

import com.cetc.kafkademo.Model.Student;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkademoApplication.class)
public class KafkademoApplicationTests {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    public RuntimeSchema<Student> schema = RuntimeSchema.createFrom(Student.class);


    @Test
    public void producer(){
//        Student student = new Student("xxx", "22");
        kafkaTemplate.send("student","{\"name\":\"leilei\",\"age\":23}");
    }

    @Test
    @Scheduled(cron = "1 0 0,0,0 * * ?")
    public void producer1(){
//        Student student = new Student("小斌斌","12");
        byte[] bytes = ProtostuffIOUtil.toByteArray(new Student(), schema,
                LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
        kafkaTemplate.send("student",bytes);
    }

}
