package a.warmup.b.unique_words;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import _other.xml.XmlStringParser;

import java.io.IOException;
import java.util.StringTokenizer;


public class UniqueWordsMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final IntWritable ONE = new IntWritable( 1 );
    /**
     * Goes through data token by token, verifies that token is a word, and removes any characters that are not letters
     * from the word.
     */
    @Override

    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        String title = new XmlStringParser( value.toString() ).getValueFromTag( "title" );

        StringTokenizer tokenizer = new StringTokenizer( title );

        while ( tokenizer.hasMoreTokens() ) {

            Text text = new Text( tokenizer.nextToken() );
            context.write( text, ONE );
        }
    }
}