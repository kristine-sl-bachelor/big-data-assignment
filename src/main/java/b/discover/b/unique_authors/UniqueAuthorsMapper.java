package b.discover.b.unique_authors;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class UniqueAuthorsMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final IntWritable ONE = new IntWritable( 1 );

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        List<String> authors = parser.getValuesFromTag( "author" );

        for( String author : authors ) {

            context.write( new Text( author ), ONE );
        }

    }
}
