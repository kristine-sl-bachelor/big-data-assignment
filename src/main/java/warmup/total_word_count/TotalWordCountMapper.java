package warmup.total_word_count;

import helpers.Word;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;
import xml.XmlStringParser;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.StringTokenizer;


public class TotalWordCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final static IntWritable ONE = new IntWritable( 1 );

    /**
     * Goes through data token by token, verifies that token is a word, and removes any characters that are not letters
     * from the word.
     */
    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        try {

            String title = new XmlStringParser( value.toString() ).getValueFromTag( "title" );

            StringTokenizer tokenizer = new StringTokenizer( title );

            while ( tokenizer.hasMoreTokens() ) {

                Text text = new Text( Word.cleanWord( tokenizer.nextToken() ) );
                context.write( text, ONE );

            }

        } catch ( ParserConfigurationException e ) {

            e.printStackTrace(); // TODO: Change this to proper handling

        } catch ( SAXException e ) {

            e.printStackTrace(); // TODO: Change this to proper handling
        }

    }
}
