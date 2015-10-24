package b.discover.c.over_50;

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
 * === ASSUMPTION ===
 *
 * In testing, we could not find any tag named "co-author", so by co-author, we assume that the task reffers to the fact that
 * one publication can have several "author" tags.
 *
 * ==================
 */

/**
 * Outputs the number of authors with over 50 registered publications in the XML document
 */
public class AuthorsPublications {

    public static final int NUMBER_OF_PUBLICATIONS = 50;

    public static void main( String[] args ) {

        try {

            System.exit( new AuthorsPublications().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( AuthorsPublications.class );
        job.setJobName( "DBLP authors with over 40 publications" );
        job.setMapperClass( AuthorsPublicationsMapper.class );
        job.setReducerClass( AuthorsPublicationsReducer.class );
        job.setInputFormatClass( XmlInputFormat.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( IntWritable.class );
        job.setOutputKeyClass( IntWritable.class );
        job.setOutputValueClass( NullWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
