package a.warmup.a.total_word_count;

import _other.helpers.Word;
import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Goes through data token by token and removes any characters that are not letters from the word,
 * before writing it to context to be counted
 */
public class TotalWordCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        List< String > titles = new XmlStringParser( value.toString() ).getValuesFromTag( "title" );

        for ( String title : titles ) {

            StringTokenizer tokenizer = new StringTokenizer( title );

            while ( tokenizer.hasMoreTokens() ) {

                Text text = new Text( Word.cleanWord( tokenizer.nextToken() ) );

                context.write( text, new IntWritable( 1 ) );
            }
        }
    }
}
