package b.discover.e.diversity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import _other.xml.XmlStringParser;

import java.io.IOException;

public class DiversityMapper extends Mapper< LongWritable, Text, Text, Text > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        String author = parser.getValueFromTag( "author" );

        while ( author != null ) {

            String type = parser.getRootTagName();

            context.write( new Text( author ), new Text( type ) );

            author = parser.getValueFromTag( "author" );
        }
    }
}
