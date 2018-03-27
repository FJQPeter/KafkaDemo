package com.cetc.kafkademo;

import com.cetc.kafkademo.Model.Student;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by FangYan on 2018/3/20.
 */
public class JsonTest {
    //json--->对象
    @Test
    public void json() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Student student = objectMapper.readValue("{\"name\":\"leilei\",\"age\":23}", Student.class);
        System.out.println(student.toString());
    }

    @Test
    public void byteTest(){
        String b = "10,6,-26,-106,-116,-26,-106,-116,18,2,49,50";
        System.out.println(b.getBytes()[1]);
    }
}
