package kr.lililli.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

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

        // 반복적으로 데이터 전송
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;

                // 프로듀서 레코드 생성(전달할 데이터)
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // 데이터 보내기 (비동기)
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // 전달된 데이터의 정보 출력
                        if (exception == null) {
                            log.info("받은 메타데이터 \n" +
                                    "Key: " + key + "\n" +
                                    "Partition: " + metadata.partition() + "\n"
                            );
                        } else {
                            log.error("프로듀서 에러", exception);
                        }
                    }
                });
            }

            
        }
        // 프로듀서 보내기 & 종료 (동기식 실행)
        producer.flush(); // '데이터 보내기'가 비동기이기에 실행 / 데이터를 보내고 난후 아래코드를 실행한다.
        producer.close(); // 프로듀서를 종료해도 데이터는 비동기이기에 계속 들어간다.
    }
}
