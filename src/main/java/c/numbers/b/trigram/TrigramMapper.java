package c.numbers.b.trigram;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Outputs all combinations of three words found in the current title
 */
public class TrigramMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        List< String > titles = new XmlStringParser( value.toString() ).getValuesFromTag( "title" );

        if ( !titles.isEmpty() ) {

            StringTokenizer tokenizer = new StringTokenizer( titles.get( 0 ) );

            if ( tokenizer.countTokens() >= 3 ) {

                String first = tokenizer.nextToken();
                String middle = tokenizer.nextToken();

                while ( tokenizer.hasMoreTokens() ) {

                    String last = tokenizer.nextToken();

                    context.write( new Text( ( first + " " + middle + " " + last ).toLowerCase() ), new IntWritable( 1 ) );

                    first = middle;
                    middle = last;
                }
            }
        }
    }
}
