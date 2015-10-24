package b.discover.a.counting;

import _other.xml.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Returns the number of different kinds of publications in the XML document
 */
public class Counting {

    public static void main( String[] args ) {

        try {

            System.exit( new Counting().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( Counting.class );
        job.setJobName( "DBLP unique authors" );
        job.setMapperClass( CountingMapper.class );
        job.setReducerClass( CountingReducer.class );
        job.setInputFormatClass( XmlInputFormat.class );

        job.setMapOutputKeyClass( Text.class );
        job.setOutputKeyClass( IntWritable.class );
        job.setOutputValueClass( NullWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
