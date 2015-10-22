package a.warmup;

import _other.helpers.DriverOutput;
import _other.helpers.XmlTags;
import a.warmup.b.unique_words.UniqueWordsMapper;
import a.warmup.b.unique_words.UniqueWordsReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UniqueWordsTest {

    private MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    private ReduceDriver< Text, IntWritable, Text, NullWritable > reduceDriver;

    private final String word1 = "Word1", word2 = "Word2";

    @Before
    public void setup() {

        UniqueWordsMapper mapper = new UniqueWordsMapper();
        UniqueWordsReducer reducer = new UniqueWordsReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        String input = XmlTags.TITLE_START
                + word1 + " " + word2
                + XmlTags.TITLE_END;

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withAllOutput( DriverOutput.getOutputTextOne( word1, word2 ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        List< IntWritable > values1 = new ArrayList< IntWritable >();
        values1.add( new IntWritable( 1 ) );

        List< IntWritable > values2 = new ArrayList< IntWritable >();
        values2.add( new IntWritable( 1 ) );
        values2.add( new IntWritable( 1 ) );

        reduceDriver.withInput( new Text( word1 ), values1 )
                .withInput( new Text( word2 ), values2 )
                .withAllOutput( DriverOutput.getOutputTextNull( word1 ) )
                .runTest();
    }
}
