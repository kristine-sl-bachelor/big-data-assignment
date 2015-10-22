package a.warmup;

import _other.helpers.DriverOutput;
import _other.helpers.StringFormat;
import _other.helpers.XmlTags;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import a.warmup.a.total_word_count.TotalWordCountMapper;
import a.warmup.a.total_word_count.TotalWordCountReducer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TotalWordCountTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, IntWritable > reduceDriver;

    @Before
    public void setUp() throws FileNotFoundException {

        TotalWordCountMapper mapper = new TotalWordCountMapper();
        TotalWordCountReducer reducer = new TotalWordCountReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMap() throws IOException {

        String word1 = "Word1", word2 = "Word2";

        String input = XmlTags.TITLE_START
                + word1 + " " + word2
                + XmlTags.TITLE_END;

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withAllOutput( DriverOutput.getOutputTextOne( word1, word2 ) )
                .runTest();
    }

    @Test
    public void testReduce() throws IOException {

        Text text = new Text( "Title" );

        IntWritable one = new IntWritable( 1 );
        IntWritable sum = new IntWritable( 10 );

        List< IntWritable > values = new ArrayList< IntWritable >();

        for ( int i = 0; i < sum.get(); i++ ) {

            values.add( one );
        }

        Text output = new Text( String.format( StringFormat.WORD, text.toString() ) );

        reduceDriver.withInput( text, values )
                .withOutput( output, sum )
                .runTest();
    }
}
