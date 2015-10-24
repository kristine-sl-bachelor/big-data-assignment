package e.search_engine;

import _other.xml.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Makes an index for each word in the XML document, with common words excluded, containing the word and a
 * collection of locations for where to find the word, seperated with ";".
 */
public class TitleIndex {

    public static void main( String[] args ) {

        try {

            System.exit( new TitleIndex().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( TitleIndex.class );
        job.setJobName( "DBLP unique authors" );
        job.setMapperClass( TitleIndexMapper.class );
        job.setReducerClass( TitleIndexReducer.class );
        job.setInputFormatClass( XmlInputFormat.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
