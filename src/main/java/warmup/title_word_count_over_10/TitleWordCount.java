package warmup.title_word_count_over_10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import warmup.unique_words.UniqueWords;
import xml.XmlInputFormat;

public class TitleWordCount {

    final static String TEMP = "temp";
    final static Configuration CONFIG = new Configuration();


    public static void main( String[] args ) {

        try {

            if ( runWordCountJob( args ) == 0 ) {

                String[] tempArgs = { TEMP, args[ 1 ] };

                int status = new TitleWordCount().run( tempArgs );

                final FileSystem fs = FileSystem.get( CONFIG );
                fs.delete( new Path( TEMP ), true );

                System.exit( status );
            }

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    private static int runWordCountJob( String[] args ) throws Exception {

        String[] tempArgs = { args[ 0 ], TEMP };

        return ToolRunner.run( new UniqueWords(), tempArgs );
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( CONFIG );

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
