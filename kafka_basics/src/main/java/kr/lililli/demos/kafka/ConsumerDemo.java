package kr.lililli.demos.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // 설정 생성
        Properties properties = new Properties();
        // 카프카 접속 정보 설정
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // 직렬화 설정
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 컨슈머 설정
        // 역직렬화 설정
        // IF) 타입이 다를 경우, 다른 직렬화 도구를 사용한다.(avro 등)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // 그룹ID
        properties.setProperty("group.id", groupId);

        // 오프셋 설정
        // none: 컨슈머 그룹이 없을때 동작 X -> 앱을 실행전 컨슈머 그룹부터 시작해야함.
        // earliest: 처음부터 메세지 전부 다 읽기 (cli 의 --from-beggining)
        // latest: 방금 보낸 새 메세지만 읽기
        properties.setProperty("auto.offset.reset", "earliest");

        // 컨슈머 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 토픽 구독하기
        consumer.subscribe(Arrays.asList(topic));

        // 데이터받기
        while (true) {
            log.info("Polling");
            // 데이터 조사시 delay 를 파라미터로 설정
            // kafka 의 과부하 방지
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record :
                    records) {
                log.info("Keys: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
