package c.numbers.b.trigram;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TrigramMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        String title = new XmlStringParser( value.toString() ).getValueFromTag( "title" );

        StringTokenizer tokenizer = new StringTokenizer( title );

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
