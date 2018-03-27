package com.cetc.kafkademo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import java.util.Properties;
import java.util.Random;

/**
 * Created by FangYan on 2018/3/26.
 *  要想实现批量发送消息的下面这两行的至关重要，第一行配置每一批的大小为16M,
 *  第二行配置每隔10ms一发送，这意味着你的主线程要能执行10ms以上才能销毁，
 *  否则无法发送消息到broker中
    props.put(“batch.size”, 16384);
    props.put(“linger.ms”, 10);
    如果你的代码在你指定的linger.ms时间内运行完了，
    那你就需要想办法增加你的程序运行时间了，我这里使用最简单的办法，让main线程挂起20ms
 *
 */
public class BatchProducer {
    private final KafkaProducer<String, String> producer;
    private final String topic="student";

    public BatchProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.104:9092");
        props.put("client.id", "DemoProducer");
        props.put("batch.size", 16384);//16M
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);//32M
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void producerMsg(){
        String data = "Apache Storm is a free and open source distributed realtime computation system Storm makes it easy to reliably process unbounded streams of data doing for realtime processing what Hadoop did for batch processing. Storm is simple, can be used with any programming language, and is a lot of fun to use!\n" +
                "Storm has many use cases: realtime analytics, online machine learning, continuous computation, distributed RPC, ETL, and more. Storm is fast: a benchmark clocked it at over a million tuples processed per second per node. It is scalable, fault-tolerant, guarantees your data will be processed, and is easy to set up and operate.\n" +
                "Storm integrates with the queueing and database technologies you already use. A Storm topology consumes streams of data and processes those streams in arbitrarily complex ways, repartitioning the streams between each stage of the computation however needed. Read more in the tutorial.";
        data = data.replaceAll("[\\pP‘’“”]", "");
        String[] words = data.split(" ");
        Random _rand = new Random();
        int events = 10;
        for (long nEvents = 0; nEvents < events; nEvents++) {
            String ip = "192.168.1.104";
            String msg = words[_rand.nextInt(words.length)];
            try {
                producer.send(new ProducerRecord<>(topic, ip, msg));
                System.out.println("Sent message: (" + ip + ", " + msg + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void test() throws InterruptedException {
        BatchProducer producer = new BatchProducer();
        producer.producerMsg();
        Thread.sleep(20);
    }
}
