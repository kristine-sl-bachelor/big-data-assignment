package c.numbers;

import c.numbers.e.method.MethodMapper;
import c.numbers.e.method.MethodReducer;
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

public class MethodTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, IntWritable, NullWritable > reduceDriver;

    @Before
    public void setup() {

        MethodMapper mapper = new MethodMapper();
        MethodReducer reducer = new MethodReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        String input = "<article>"
                + "<title>Method METHOD MetHod method</title>"
                + "</article>";

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( "method" ), new IntWritable( 1 ) )
                .withOutput( new Text( "method" ), new IntWritable( 1 ) )
                .withOutput( new Text( "method" ), new IntWritable( 1 ) )
                .withOutput( new Text( "method" ), new IntWritable( 1 ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        List<IntWritable> values = new ArrayList< IntWritable >(  );

        int numberOfWords = 3;

        for( int i = 0; i < numberOfWords; i++) {

            values.add( new IntWritable( 1 ) );
        }

        Text useless = new Text( "method" );

        reduceDriver.withInput( useless, values )
                .withOutput( new IntWritable( numberOfWords ), NullWritable.get() )
                .runTest();
    }
}
