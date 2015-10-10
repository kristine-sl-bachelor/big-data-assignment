package warmup.word_character_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import warmup.unique_word_count.UniqueWordCount;

public class WordCharacterCount {

    public static void main( String[] args ) {

        try {

            JobConf wordlistJob = new JobConf( new Configuration(), UniqueWordCount.class );


            final Job job = Job.getInstance( new Configuration() );

            job.setJarByClass( WordCharacterCount.class );
            job.setJobName( "DBLP word counter" );

            FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
            FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

            job.setMapperClass( WordCharacterCountMapper.class );
            job.setReducerClass( WordCharacterCountReducer.class );
            job.setOutputKeyClass( IntWritable.class );            // Word
            job.setOutputValueClass( NullWritable.class );   // Sum

            System.exit( job.waitForCompletion( true ) ? 0 : 1 );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }
}
