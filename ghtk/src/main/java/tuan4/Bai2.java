package tuan4;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Bai2 {
    // Map function
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] stringArr = value.toString().split("\\s+");
            for (String str : stringArr) {
                int number = Integer.parseInt(str);
                context.write(new IntWritable(0),new IntWritable(number));
            }
        }
    }


    // Reduce function
    public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Boolean> arrayCheck=new ArrayList<Boolean>(Arrays.asList(new Boolean[10000000]));
            Collections.fill(arrayCheck, Boolean.TRUE);
            int sum = 0;
            for (IntWritable val : values) {
                int indexCheck = val.get();
                if (arrayCheck.get(indexCheck)){
                    arrayCheck.set(indexCheck, false);
                    sum++;
                }
            }

//            v2
//            HashSet<String> setA = new HashSet<String>();
//
//            for(IntWritable s : values) {
//                setA.add((s.toString()));
//            }
//            result.set(setA.size());
//
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(Bai2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}