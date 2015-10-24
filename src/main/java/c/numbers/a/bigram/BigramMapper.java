package c.numbers.a.bigram;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Outputs all combinations of two words found in titles, so that they can be counted
 */
public class BigramMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        List< String > titles = new XmlStringParser( value.toString() ).getValuesFromTag( "title" );

        if ( !titles.isEmpty() ) {

            StringTokenizer tokenizer = new StringTokenizer( titles.get( 0 ) );

            String previousToken = tokenizer.nextToken();

            while ( tokenizer.hasMoreTokens() ) {

                String currentToken = tokenizer.nextToken();

                context.write( new Text( ( previousToken + " " + currentToken ).toLowerCase() ), new IntWritable( 1 ) );

                previousToken = currentToken;
            }
        }
    }
}
