package a.warmup;

import _other.helpers.DriverOutput;
import _other.helpers.XmlTags;
import a.warmup.d.stopword_exclude.StopwordExclude;
import a.warmup.d.stopword_exclude.StopwordExcludeMapper;
import a.warmup.d.stopword_exclude.StopwordExcludeReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StopwordExcludeTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, IntWritable > reduceDriver;

    @Before
    public void setup() throws IOException {

        StopwordExcludeMapper mapper = new StopwordExcludeMapper();
        StopwordExcludeReducer reducer = new StopwordExcludeReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );

    }

    @Test
    public void testMapper() throws IOException {

        String word1 = "Word1", word2 = "Word2";

        String input = XmlTags.TITLE_START
                + word1 + " " + word2
                + XmlTags.TITLE_END;

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withAllOutput( DriverOutput.getOutputTextOne( word1, word2 ) )
                .runTest();
    }

    // TODO: Find way to test MultipleOutputs
    @Ignore
    @Test
    public void testReducer() throws IOException {

        Job job = Job.getInstance( reduceDriver.getContext().getConfiguration() );

        new StopwordExclude().setupMultiOutput( job );

        List<IntWritable> values = new ArrayList< IntWritable >(  );
        values.add( new IntWritable( 1 ) );

        String inList = "A", notInList = "Test";

        reduceDriver.withInput( new Text( inList ), values )
                .withInput( new Text( notInList ), values )
                .withMultiOutput( StopwordExclude.FILE_NAME, new Text( notInList ), new IntWritable( 1 ) )
                .runTest();
    }
}
