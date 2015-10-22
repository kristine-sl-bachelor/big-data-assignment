package a.warmup.d.stopword_exclude;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StopwordExclude {

    public final static String FILE_NAME = "PopularWords";
    public final static String PATH = "popular";

    public static void main( String[] args ) {

        try {

            System.exit( new StopwordExclude().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( StopwordExclude.class );
        job.setJobName( "Excludes specific words from unique words" );
        job.setMapperClass( StopwordExcludeMapper.class );
        job.setReducerClass( StopwordExcludeReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( PATH ) );

        setupMultiOutput( job );

        return job.waitForCompletion( true ) ? 0 : 1;
    }

    public void setupMultiOutput( Job job ) {

        MultipleOutputs.addNamedOutput( job, FILE_NAME, TextOutputFormat.class, Text.class, IntWritable.class );
    }
}
