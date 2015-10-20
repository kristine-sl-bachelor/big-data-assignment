package warmup.title_word_count_over_10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import xml.XmlInputFormat;

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

        final Job job = Job.getInstance( new Configuration(  ) );

        job.setJarByClass( TitleWordCount.class );
        job.setJobName( "DBLP word character counter" );
        job.setMapperClass( TitleWordCountMapper.class );
        job.setReducerClass( TitleWordCountReducer.class );

        job.setMapperClass( TitleWordCountMapper.class );
        job.setReducerClass( TitleWordCountReducer.class );

        job.setOutputKeyClass( IntWritable.class );
        job.setOutputValueClass( NullWritable.class );

        job.setInputFormatClass( XmlInputFormat.class );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
