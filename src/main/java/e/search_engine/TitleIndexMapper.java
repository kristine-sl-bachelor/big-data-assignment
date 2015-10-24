package e.search_engine;

import _other.helpers.Word;
import _other.xml.XmlStringParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TitleIndexMapper extends Mapper< LongWritable, Text, Text, Text > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );
        Text attributeKey = new Text( parser.getRootAttributeType() );

        String title = parser.getValueFromTag( "title" );

        StringTokenizer tokenizer = new StringTokenizer( title );

        while ( tokenizer.hasMoreTokens() ) {

            context.write( new Text( Word.cleanWord( tokenizer.nextToken() ).toLowerCase() ), attributeKey );
        }
    }
}
