package b.discover.g.names;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class NamesMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        List<String> authors = parser.getValuesFromTag( "author" );

        for( String author: authors ) {

            StringTokenizer tokenizer = new StringTokenizer( author );

            context.write( new Text( tokenizer.nextToken() ), Names.FIRST_NAME );

            while( tokenizer.hasMoreTokens() ) {

                context.write( new Text( tokenizer.nextToken() ), Names.LAST_NAME );
            }
        }
    }
}
