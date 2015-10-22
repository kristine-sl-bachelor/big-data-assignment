package b.discover;

import b.discover.f.topic.Topic;
import b.discover.f.topic.TopicMapper;
import b.discover.f.topic.TopicReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopicTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, IntWritable > reduceDriver;
    private List< String > words;
    private final String WORD = "Word";

    @Before
    public void setup() {

        TopicMapper mapper = new TopicMapper();
        TopicReducer reducer = new TopicReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );

        words = new ArrayList< String >();

        for ( int i = 0; i < Topic.NUMBER_OF_TOPICS * 2; i++ ) {

            words.add( WORD + i );
        }
    }

    @Test
    public void testMapper() throws IOException {

        String input = "<article>"
                + "<title>" + words.get( 0 ) + " " + words.get( 1 ) + "</title>"
                + "</article>";

        IntWritable one = new IntWritable( 1 );

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( words.get( 0 ) ), one )
                .withOutput( new Text( words.get( 1 ) ), one )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        for( int i = 0; i < words.size(); i++) {

            reduceDriver.withInput( new Text( words.get( i ) ), getInputList( i + 1 ) );
        }

        for( int i = words.size(); i > words.size() - Topic.NUMBER_OF_TOPICS; i-- ) {

            reduceDriver.withOutput( new Text( words.get( i - 1 ) ), new IntWritable( i ) );
        }

        reduceDriver.runTest();
    }

    private List< IntWritable > getInputList( int sum ) {

        List<IntWritable> output = new ArrayList< IntWritable >(  );

        for( int i = 0; i < sum; i++ ) {

            output.add( new IntWritable( 1 ) );
        }

        return output;
    }
}
