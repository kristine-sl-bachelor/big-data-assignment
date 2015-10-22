package b.discover.b.unique_authors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import _other.xml.XmlStringParser;

import java.io.IOException;

public class UniqueAuthorsMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final IntWritable ONE = new IntWritable( 1 );

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        String author = parser.getValueFromTag( "author" );

        while ( author != null ) { // TODO: Remove all of these if you don't need them

            context.write( new Text( author ), ONE );

            author = parser.getValueFromTag( "author" );
        }

    }
}
