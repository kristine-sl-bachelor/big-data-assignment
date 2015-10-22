package b.discover.e.diversity;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class DiversityMapper extends Mapper< LongWritable, Text, Text, Text > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        List<String> authors = parser.getValuesFromTag( "author" );

        String type = parser.getRootTagName();

        for( String author : authors ) {

            context.write( new Text( author ), new Text( type ) );
        }
    }
}
