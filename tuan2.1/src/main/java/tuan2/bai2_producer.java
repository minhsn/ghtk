package tuan2;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class bai2_producer {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("/home/dell/Desktop/tuan2.1/src/main/avro/emp.avsc"));
        File file=new File("/home/dell/Desktop/tuan2.1/src/main/avro/a.avro");
        String topic="minhnd95";
        GenericRecord e1=new GenericData.Record(schema);
        GenericData.Record ad = new GenericData.Record(schema.getField("adress").schema());
        ad.put("pinCode", 11);
        ad.put("streetName","cau giay");
        e1.put("ID",16);
        e1.put("email","minhsn89");
        e1.put("name","minh");
        e1.put("adress",ad);


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(e1,binaryEncoder);
        binaryEncoder.flush();
        out.close();
        byte[] serializedBytes = out.toByteArray();



        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer<String, byte[]> producer=new KafkaProducer<String,byte[]>(properties);

        ProducerRecord<String, byte[]> producerRecord= new ProducerRecord<String,byte[]>(topic,null,serializedBytes);
        producer.send(producerRecord);
        producer.flush();
        producer.close();

    }
}
