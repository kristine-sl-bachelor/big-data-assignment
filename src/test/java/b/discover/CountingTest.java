package b.discover;

import b.discover.a.counting.CountingMapper;
import b.discover.a.counting.CountingReducer;
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

public class CountingTest {

    MapDriver< LongWritable, Text, Text, NullWritable > mapDriver;
    ReduceDriver< Text, NullWritable, IntWritable, NullWritable > reduceDriver;

    private final String TYPE = "type";

    @Before
    public void setup() {

        CountingMapper mapper = new CountingMapper();
        CountingReducer reducer = new CountingReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        String input = "<" + TYPE + ">"
                + "<data>Data!</data>"
                + "</" + TYPE + ">";

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( TYPE ), NullWritable.get() )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        String one = "a", two = "b", three = "c";

        List< NullWritable > nullWritable = new ArrayList< NullWritable >();
        nullWritable.add( NullWritable.get() );

        reduceDriver.withInput( new Text( TYPE + one ), nullWritable )
                .withInput( new Text( TYPE + two ), nullWritable )
                .withInput( new Text( TYPE + three ), nullWritable )
                .withOutput( new IntWritable( 3 ), NullWritable.get() )
                .runTest();

    }
}
