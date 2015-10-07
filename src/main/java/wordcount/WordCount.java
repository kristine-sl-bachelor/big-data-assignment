package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main( String[] args ) {

        try {

            final Job job = Job.getInstance( new Configuration() );

            job.setJarByClass(WordCount.class);
            job.setJobName("DBLP word counter");

            FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
            FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {

            e.printStackTrace();
            System.exit(-1);
        }
    }
}
