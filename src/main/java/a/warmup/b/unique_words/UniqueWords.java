package a.warmup.b.unique_words;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import _other.xml.XmlInputFormat;

public class UniqueWords {

    public static void main( String[] args ) {

        try {

            System.exit( new UniqueWords().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }

    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( UniqueWords.class );
        job.setJobName( "DBLP unique word counter" );
        job.setMapperClass( UniqueWordsMapper.class );
        job.setReducerClass( UniqueWordsReducer.class );
        job.setInputFormatClass( XmlInputFormat.class );

        job.setMapOutputValueClass( IntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( NullWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
