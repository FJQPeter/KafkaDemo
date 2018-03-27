package com.cetc.kafkademo.Model;

import lombok.Data;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by FangYan on 2018/3/20.
 */
@Data
public class Student{


    private String name;

    private String age;

/*    public Student(String name, String age){
        this.name=name;
        this.age=age;
    }

    public Student() {
        super();
    }*/
}
