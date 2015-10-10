package warmup.unique_word_count_test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class UniqueWordCountMapper extends Mapper< LongWritable, Text, Text, NullWritable > {

    private final Text WORD = new Text();

    /**
     * Goes through data token by token, verifies that token is a word, and removes any characters that are not letters
     * from the word.
     */
    @Override

    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        String text = value.toString();
        StringTokenizer tokenizer = new StringTokenizer( text );

        while ( tokenizer.hasMoreTokens() ) {

            WORD.set( tokenizer.nextToken() );

            if ( Integer.parseInt( tokenizer.nextToken() ) == 1 ) {

                context.write( WORD, NullWritable.get() );
            }
        }
    }
}