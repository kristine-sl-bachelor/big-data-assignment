package a.warmup.c.title_word_count_over_10;

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
 * Outputs the number of titles in the XML document which consists of more that 10 words
 */
public class TitleWordCount {

    public static void main( String[] args ) {

        try {

            System.exit( new TitleWordCount().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( TitleWordCount.class );
        job.setJobName( "DBLP word character counter" );
        job.setMapperClass( TitleWordCountMapper.class );
        job.setReducerClass( TitleWordCountReducer.class );
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
