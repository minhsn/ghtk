package tuan2;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class bai1_SERIALIZER {
    public static void main(String args[]) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("/home/dell/Desktop/tuan2.1/src/main/avro/emp.avsc"));
        GenericRecord e1 = new GenericData.Record(schema);
        GenericData.Record ad = new GenericData.Record(schema.getField("adress").schema());
        ad.put("pinCode", 11);
        ad.put("streetName","cau giay");

        e1.put("ID",16);
        e1.put("email","minhsn89");
        e1.put("name","minh");
        e1.put("adress",ad);
        DatumWriter<GenericRecord> datumWriter=new SpecificDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, new File("/home/dell/Desktop/tuan2.1/src/main/avro/a.avro"));
        dataFileWriter.append(e1);
        dataFileWriter.close();
    }

}
