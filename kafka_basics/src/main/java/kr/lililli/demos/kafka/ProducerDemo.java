package kr.lililli.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Producer");

        // 프로듀서 설정 생성
        Properties properties = new Properties();
        // 카프카 접속 정보 설정
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // 직렬화 설정
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 프로듀서 레코드 생성(전달할 데이터)
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        // 데이터 보내기 (비동기)
        producer.send(producerRecord);

        // 프로듀서 보내기 & 종료 (동기식 실행)
        producer.flush(); // '데이터 보내기'가 비동기이기에 실행 / 데이터를 보내고 난후 아래코드를 실행한다.
        producer.close(); // 프로듀서를 종료해도 데이터는 비동기이기에 계속 들어간다.
    }
}
