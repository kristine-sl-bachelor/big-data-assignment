package e.search_engine;

import _other.helpers.Word;
import _other.xml.XmlStringParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Gets the location for the word from "type" on the root tag
 */
public class TitleIndexMapper extends Mapper< LongWritable, Text, Text, Text > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );
        Text attributeKey = new Text( parser.getRootAttributeKey() );

        List< String > titles = parser.getValuesFromTag( "title" );

        if ( !titles.isEmpty() ) {

            StringTokenizer tokenizer = new StringTokenizer( titles.get( 0 ) );

            while ( tokenizer.hasMoreTokens() ) {

                context.write( new Text( Word.cleanWord( tokenizer.nextToken() ).toLowerCase() ), attributeKey );
            }
        }
    }
}
