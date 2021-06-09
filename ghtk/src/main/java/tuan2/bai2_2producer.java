package tuan2;

import com.sun.org.apache.xml.internal.serializer.utils.SerializerMessages;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class bai2_2producer {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("/home/dell/Desktop/tuan2.1/src/main/avro/emp.avsc"));
        String topic="minhnd855";

        GenericRecord e1=new GenericData.Record(schema);
        GenericData.Record ad = new GenericData.Record(schema.getField("adress").schema());
        ad.put("pinCode", 11);
        ad.put("streetName","cau giay");
        e1.put("ID",16);
        e1.put("email","minhsn89");
        e1.put("name","minh");
        e1.put("adress",ad);



        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.13:8081");

        KafkaProducer<String, GenericRecord> producer=new KafkaProducer<String,GenericRecord>(properties);

        ProducerRecord<String, GenericRecord> producerRecord= new ProducerRecord<String, GenericRecord>(topic,e1);
        producer.send(producerRecord);
        producer.close();

    }
}

