package com.cetc.kafkademo.kafka;

import com.cetc.kafkademo.Mapper.TestMapper;
import com.cetc.kafkademo.Model.Student;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

/**
 * B'在class字节码中表示byte数组,
 * Created by FangYan on 2018/3/14.
 */
@Controller
public class Producer {
    @Autowired private KafkaTemplate kafkaTemplate;

    @Resource private TestMapper testMapper;

    public RuntimeSchema<Student> schema = RuntimeSchema.createFrom(Student.class);

    //发送消息
   /* @RequestMapping("/producer")
    @Scheduled(fixedDelay = 1000)
    public void send(){
        Student student = new Student("小斌斌","12");
        byte[] bytes = ProtostuffIOUtil.toByteArray(student, schema,
                LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
        kafkaTemplate.send("student",bytes);
    }*/

    @RequestMapping("/producer")
    @ResponseBody
    public String enroll(Student student){
       byte[] bytes = ProtostuffIOUtil.toByteArray(student, schema,
               LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
       kafkaTemplate.send("student",bytes);
       return "success";
   }

    @RequestMapping("/form")
    @ResponseBody
    public String form(Student student) throws InterruptedException {
        testMapper.addStudent(student);
        Thread.sleep(1000);
        return "success";
   }
}
