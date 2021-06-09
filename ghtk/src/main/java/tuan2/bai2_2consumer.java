package tuan2;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class bai2_2consumer {
    public static void main(String[] args) {
        Properties properties =new Properties();
        properties.put("bootstrap.servers","10.140.0.13:9092");
        properties.put("group.id","minh7");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.13:8081");



        KafkaConsumer<String,GenericRecord> consumers=new KafkaConsumer<String, GenericRecord>(properties);
        consumers.subscribe(Arrays.asList("minhnd855"));
        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumers.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, GenericRecord> record : records) {
                System.out.println("Message id: " + record.value().get("ID"));
                System.out.println("Message : " + record.value());
            }
            consumers.commitAsync();


        }


    }

}

