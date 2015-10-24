package a.warmup.a.total_word_count;

import _other.xml.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * === TRUE FOR ALL JOBS ===
 *
 * Implemented with run( String[] args) method, so that it is easily configurable if one needs to run one of the jobs
 * with {@link org.apache.hadoop.util.ToolRunner#run(Tool, String[])}. All that is needed is to extend
 * {@link org.apache.hadoop.conf.Configured} and implement {@link org.apache.hadoop.util.Tool}.
 *
 * =========================
 */

/**
 * Prints a list of all the words in all the titles of the XML document, with a counter for the number of times the word occures in
 * the document.
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
        job.setJobName( "Word counter" );
        job.setMapperClass( TotalWordCountMapper.class );
        job.setReducerClass( TotalWordCountReducer.class );

        // Custom InputFormat class for reading XML files
        job.setInputFormatClass( XmlInputFormat.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
