package warmup.stopword_exclude;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import warmup.unique_words.UniqueWords;

public class StopwordExclude {

    private final static String TEMP = "temp";
    private final static Configuration CONFIG = new Configuration();

    protected final static String FILE_NAME = "PopularWords";
    protected final static String PATH = "popular";

    public static void main( String[] args ) {

        try {

            if ( runUniqueWordsJob( args ) == 0 ) {

                String[] tempArgs = { TEMP, args[ 1 ] };

                FileSystem fileSystem = FileSystem.get( CONFIG );
                fileSystem.delete( new Path( PATH ), true );

                int status = new StopwordExclude().run( tempArgs );

                fileSystem.delete( new Path( TEMP ), true );

                System.exit( status );
            }

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    private static int runUniqueWordsJob( String[] args ) throws Exception {

        String[] tempArgs = { args[ 0 ], TEMP };

        return ToolRunner.run( new UniqueWords(), tempArgs );
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( CONFIG );

        job.setJarByClass( StopwordExclude.class );
        job.setJobName( "Excludes specific words from unique words" );
        job.setMapperClass( StopwordExcludeMapper.class );
        job.setReducerClass( StopwordExcludeReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( NullWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        MultipleOutputs.addNamedOutput( job, FILE_NAME, TextOutputFormat.class, Text.class, NullWritable.class );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
