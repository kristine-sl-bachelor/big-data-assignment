package c.numbers.a.bigram;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class BigramMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final IntWritable ONE = new IntWritable( 1 );

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        String title = new XmlStringParser( value.toString() ).getValueFromTag( "title" );

        StringTokenizer tokenizer = new StringTokenizer( title );

        String previousToken = tokenizer.nextToken();

        while ( tokenizer.hasMoreTokens() ) {

            String currentToken = tokenizer.nextToken();

            context.write( new Text( ( previousToken + " " + currentToken ).toLowerCase() ), ONE );

            previousToken = currentToken;
        }
    }
}
