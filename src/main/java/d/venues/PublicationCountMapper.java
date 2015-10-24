package d.venues;

import _other.helpers.Word;
import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;

import java.io.IOException;

/**
 * Iterates through all the child nodes of the publication tag, and outputs the value and publication year for all nodes where the
 * node value is not a number ( aka. where it starts with a letter )
 */
public class PublicationCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        NodeList nodes = parser.getChildElements();

        int year = Integer.parseInt( parser.getValuesFromTag( "year" ).get( 0 ) );

        for ( int i = 0; i < nodes.getLength(); i++ ) {

            String nodeValue = nodes.item( i ).getNodeValue().trim();

            if ( Word.startsWithLetter( nodeValue ) ) {

                context.write( new Text( nodeValue ), new IntWritable( year ));
            }
        }
    }
}
