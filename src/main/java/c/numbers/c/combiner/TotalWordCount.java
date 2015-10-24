package c.numbers.c.combiner;

import _other.xml.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task a.a with a combiner, to study the effects.
 *
 * As pointed out below, this enables the Job to reduce the output for each split, in order to minimize the number of values
 * passed onto the final reducer.
 */
public class TotalWordCount {

    public static void main( String[] args ) {

        try {

            System.exit( new TotalWordCount().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        Configuration config = new Configuration();

        final Job job = Job.getInstance( config );

        job.setJarByClass( TotalWordCount.class );
        job.setJobName( "Word counter with combiner" );
        job.setMapperClass( TotalWordCountMapper.class );
        job.setReducerClass( TotalWordCountReducer.class );
        job.setInputFormatClass( XmlInputFormat.class );

        /**
         * Finds the sum for each word before final reduce, to minimize the number of iterations through values
         * needed for final reduce job.
         *
         * Uses the same reducer, as we just need the sum of the values, and it therefore doesn't matter if these
         * are summed up more than once during the process.
         */
        job.setCombinerClass( TotalWordCountReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
