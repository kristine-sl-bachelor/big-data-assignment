package b.discover.g.names;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import _other.xml.XmlInputFormat;

/**
 * Outputs the top 5 most found first- and last names found for authors in the XML document
 *
 * Uses predefined values to differentiate between first and last names, represented by numbers so that it easy to add
 * additional values, like middle name, if wanted.
 */
public class Names {

    public static final IntWritable FIRST_NAME = new IntWritable( 0 ), LAST_NAME = new IntWritable( 1 );

    public static final int NUMBER_OF_AUTHORS = 5;

    public static void main( String[] args ) {

        try {

            System.exit( new Names().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( Names.class );
        job.setJobName( "DBLP diversity" );
        job.setMapperClass( NamesMapper.class );
        job.setReducerClass( NamesReducer.class );
        job.setInputFormatClass( XmlInputFormat.class );

        job.setMapOutputValueClass( IntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
