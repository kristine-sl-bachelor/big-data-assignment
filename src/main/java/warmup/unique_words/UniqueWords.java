package warmup.unique_words;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import warmup.word_count.WordCount;

public class UniqueWords extends Configured implements Tool {

    final static String TEMP = "temp";
    final static Configuration CONFIG = new Configuration();


    public static void main( String[] args ) {

        try {

            if ( runWordCountJob( args ) == 0 ) {

                String[] tempArgs = { TEMP, args[ 1 ] };

                int status = new UniqueWords().run( tempArgs );

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

        return ToolRunner.run( new WordCount(), tempArgs );
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( CONFIG );

        job.setJarByClass( UniqueWords.class );
        job.setJobName( "DBLP unique word counter" );
        job.setMapperClass( UniqueWordCountMapper.class );
        job.setReducerClass( UniqueWordCountReducer.class );

        job.setOutputKeyClass( Text.class );                // Word
        job.setOutputValueClass( NullWritable.class );      // No sum

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
