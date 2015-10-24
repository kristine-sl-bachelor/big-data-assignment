package d.venues;

import _other.helpers.Word;
import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;

import java.io.IOException;

public class PublicationCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        XmlStringParser parser = new XmlStringParser( value.toString() );

        NodeList nodes = parser.getChildElements();

        int year = Integer.parseInt( parser.getValueFromTag( "year" ) );

        for ( int i = 0; i < nodes.getLength(); i++ ) {

            String nodeValue = nodes.item( i ).getNodeValue().trim();

            if ( Word.isWord( nodeValue ) ) {

                context.write( new Text( nodeValue ), new IntWritable( year ));
            }
        }
    }
}
