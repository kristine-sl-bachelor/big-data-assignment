package warmup.word_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final static IntWritable ONE = new IntWritable( 1 );

    /**
     * Goes through data token by token, verifies that token is a word, and removes any characters that are not letters
     * from the word.
     */
    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        context.write( value, ONE );

        /*
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        try {

            DocumentBuilder builder = factory.newDocumentBuilder();
            InputSource inputSource = new InputSource( new StringReader( value.toString() ) );
            Document document = builder.parse( inputSource );

            NodeList list = document.getChildNodes();

            for ( int i = 0; i < list.getLength(); i++ ) {

                String word = list.item( i ).getNodeName();

                if ( Word.isWord( word ) ) {

                    context.write( new Text( Word.cleanWord( word ) ), ONE );
                }
            }

        } catch ( ParserConfigurationException e ) {

            e.printStackTrace();
        } catch ( SAXException e ) {
            e.printStackTrace();
        }
        */

    }
}
