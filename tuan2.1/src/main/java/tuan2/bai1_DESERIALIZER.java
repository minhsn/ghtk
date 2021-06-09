package tuan2;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class bai1_DESERIALIZER {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("/home/dell/Desktop/tuan2.1/src/main/avro/emp.avsc"));

        DatumReader<GenericRecord> datumReader=new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new  File("/home/dell/Desktop/tuan2.1/src/main/avro/a.avro"), datumReader);
        GenericRecord emp = null;
        while (dataFileReader.hasNext()) {
            emp = dataFileReader.next(emp);
            System.out.println(emp);
        }

    }
}
