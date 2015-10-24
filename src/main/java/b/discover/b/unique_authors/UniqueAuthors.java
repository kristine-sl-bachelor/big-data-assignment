package b.discover.b.unique_authors;

import _other.xml.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * === ASSUMPTION ===
 *
 * It is assumed that by "unique authors", the tasks means to just print each author once, not matter how many times they appear in the
 * XML document. If it is meant that the tasks should output authors who only appear once in the document, it would simply be to
 * implement same process as in {@link a.warmup.b.unique_words.UniqueWords}, and only output the authors with sum == 1 in reducer.
 *
 * ==================
 */
public class UniqueAuthors {

    public static void main( String[] args ) {

        try {

            System.exit( new UniqueAuthors().run( args ) );

        } catch ( Exception e ) {

            e.printStackTrace();
            System.exit( -1 );
        }
    }

    public int run( String[] args ) throws Exception {

        final Job job = Job.getInstance( new Configuration() );

        job.setJarByClass( UniqueAuthors.class );
        job.setJobName( "DBLP unique authors" );
        job.setMapperClass( UniqueAuthorsMapper.class );
        job.setReducerClass( UniqueAuthorsReducer.class );
        job.setInputFormatClass( XmlInputFormat.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( NullWritable.class );

        FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
