

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Bai3_c1 {
    // Map function
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Splitting the line on spaces
            String s="";
            String[] stringArr = value.toString().split(",");
            s=value.toString();
            if (stringArr[5].equals("doctor")){
                context.write(new Text(stringArr[5]),new Text(s+","+10000000));
            }
            if (stringArr[5].equals("police officer")){
                context.write(new Text(stringArr[5]),new Text(s+","+18000000));
            }
            if (stringArr[5].equals("worker")){
                context.write(new Text(stringArr[5]),new Text(s+","+12000000));
            }
            if (stringArr[5].equals("developer")){
                context.write(new Text(stringArr[5]),new Text(s+","+14000000));
            }
            if (stringArr[5].equals("teacher")){
                context.write(new Text(stringArr[5]),new Text(s+","+15000000));
            }
            if (stringArr[5].equals("farmer")){
                context.write(new Text(stringArr[5]),new Text(s+","+6000000));
            }
            if (stringArr[5].equals("baker")){
                context.write(new Text(stringArr[5]),new Text(s+","+10000000));
            }
            if (stringArr[5].equals("firefighter")){
                context.write(new Text(stringArr[5]),new Text(s+","+11000000));
            }
            if (stringArr[5].equals("driver")) {
                context.write(new Text(stringArr[5]),new Text(s+","+4000000));
            }

        }
    }

    // Reduce function
    public static class MyReducer extends Reducer<Text, Text, NullWritable, Text> {
        private StringBuilder result = new StringBuilder();
        private String header = "id,city,email,firstName,lastName,fieldName,Salary";
        private Boolean is_header = true;


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if(is_header){
                context.write(NullWritable.get(),new Text(header));
                is_header = false;
            }
            for (Text val : values) {
                context.write(NullWritable.get(),val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(Bai3_c1.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}