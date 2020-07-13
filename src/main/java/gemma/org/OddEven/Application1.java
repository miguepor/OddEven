package gemma.org.OddEven;


import java.io.IOException;
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



public class Application1 {
    public static class CountPhrases extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int num_words = context.getConfiguration().getInt("num_words",2);
            StringTokenizer itr = new StringTokenizer(value.toString());
             String[] words;
            String nwords= new String();
             int count = 0;
            if(itr.hasMoreTokens() && (itr.countTokens()>=num_words)) {
                words = new String[num_words];
                while (count < num_words) {
                    words[count] = itr.nextToken();
                    count++;
                }
                for (String a:
                        words) {
                    nwords+=a+" ";
                }
                Text phrase = new Text(nwords);
                context.write(phrase,one);
                while (itr.hasMoreTokens()){
                    for(int i=0;i<count-1;i++){
                        words[i]=words[i+1];
                    }
                    words[count-1]=itr.nextToken();
                    for (String a:
                            words) {
                        nwords+=a+" ";
                    }
                    phrase= new Text(nwords);
                    nwords="";
                    context.write(phrase,one);
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
        conf.setInt("num_words", Integer.parseInt(args[2]));

        if(args.length != 3) {
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