package b.discover.d.activity_year;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class ActivityYearMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        List<String> authors = parser.getValuesFromTag( "author" );

        int year = Integer.parseInt( parser.getValueFromTag( "year" ) );

        for( String author : authors ) {

            context.write( new Text( author ), new IntWritable( year ) );
        }

    }
}
