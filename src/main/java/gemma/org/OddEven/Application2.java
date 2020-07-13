package gemma.org.OddEven;


import java.io.IOException;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;



public class Application2 {
    public static class CountPhrases extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        boolean found =false;
        private final static IntWritable zero = new IntWritable(0);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String conc_words = context.getConfiguration().get("conc_words");
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] words= conc_words.split(",");

     if(itr.hasMoreTokens()){

                for (String aux:
                     words) {

                     itr=new StringTokenizer(value.toString());

                    while (itr.hasMoreTokens()) {

                        if (aux.equalsIgnoreCase(itr.nextToken())) {
                            Text palabra = new Text(aux);
                            context.write(palabra, one);
                            found = true;
                        }
                    }
                        if(!found){
                            Text palabra= new Text(aux);
                            context.write(palabra,zero);
                        }

                    }


                }
            }

        }


    public static class SumWordCount extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapred.job.queue.name", "default");
        //conf.set("custom_arguments_name", "custom_string");
        StringJoiner words = new StringJoiner(",");


        for(int i=2;i<args.length;i++){

          words.add(args[i]);
        }

        String conc_words=words.toString();

        conf.set("conc_words",conc_words);

        if(args.length < 3) {
            throw new RuntimeException("Need two arguments (input and output folders on HDFS)");
        }
        Path input_folder  = new Path( args[0] );
        Path output_folder = new Path( args[1] );



        // configuration should contain reference to your namenode
        FileSystem fs = FileSystem.get(new Configuration());
        // true stands for recursively deleting the folder you gave
        if(!fs.exists(input_folder)) {
            throw new RuntimeException("Input folder does not exist on HDFS filesystem");
        }
        if(fs.exists(output_folder)) {
            //throw new RuntimeException("Output folder already exist on HDFS filesystem");
            fs.delete(output_folder, true);
        }
        Job job = Job.getInstance(conf, "Summing of occurrences of the phrases ");

        job.setJarByClass(Application1.class);
        job.setMapperClass(CountPhrases.class);
        job.setReducerClass(SumWordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input_folder);
        FileOutputFormat.setOutputPath(job, output_folder);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}