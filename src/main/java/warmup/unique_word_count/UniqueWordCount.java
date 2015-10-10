package warmup.unique_word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueWordCount {

    public static void main( String[] args ) {

        try {

            final Job job = Job.getInstance( new Configuration() );

            job.setJarByClass( UniqueWordCount.class );
            job.setJobName( "DBLP unique word counter" );

            FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
            FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

            job.setMapperClass( UniqueWordCountMapper.class );
            job.setReducerClass( UniqueWordCountReducer.class );
            job.setOutputKeyClass( Text.class );                // Word
            job.setMapOutputValueClass( IntWritable.class );    // Sum for reduce
            job.setOutputValueClass( NullWritable.class );      // No sum for print

            System.exit( job.waitForCompletion( true ) ? 0 : 1 );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }
}
