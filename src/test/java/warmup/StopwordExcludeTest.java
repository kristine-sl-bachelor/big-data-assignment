package warmup;

import helpers.DriverOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import warmup.stopword_exclude.StopwordExclude;
import warmup.stopword_exclude.StopwordExcludeMapper;
import warmup.stopword_exclude.StopwordExcludeReducer;

import java.io.IOException;

public class StopwordExcludeTest {

    MapDriver< LongWritable, Text, Text, NullWritable > mapDriver;
    ReduceDriver< Text, NullWritable, Text, NullWritable > reduceDriver;

    @Before
    public void setup() throws IOException {

        new StopwordExclude(); // Initializes MultipleOutputs

        StopwordExcludeMapper mapper = new StopwordExcludeMapper();
        StopwordExcludeReducer reducer = new StopwordExcludeReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );

    }

    @Test
    public void testMapper() throws IOException {

        String title = "Title,\n";
        String titleClean = "Title";
        String input = title + title + title;

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withAllOutput( DriverOutput.getOutputTextNull( titleClean, titleClean, titleClean ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        // TODO: Implement test that works with MultipleOutputs

        /*
        List<NullWritable> nullList = new ArrayList< NullWritable >(  );
        nullList.add( NullWritable.get() );

        String inList = "A", notInList = "Test";

        reduceDriver.withInput( new Text( inList ), nullList )
                .withInput( new Text( notInList ), nullList )
                .withOutput( new Text( notInList ), NullWritable.get() )
                .runTest();
       */
    }
}
