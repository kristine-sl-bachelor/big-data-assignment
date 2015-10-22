package b.discover.g.names;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import _other.xml.XmlStringParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class NamesMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        String author = parser.getValueFromTag( "author" );

        while ( author != null ) {

            StringTokenizer tokenizer = new StringTokenizer( author );

            context.write( new Text( tokenizer.nextToken() ), Names.FIRST_NAME );

            while( tokenizer.hasMoreTokens() ) {

                context.write( new Text( tokenizer.nextToken() ), Names.LAST_NAME );
            }

            author = parser.getValueFromTag( "author" );
        }
    }
}
