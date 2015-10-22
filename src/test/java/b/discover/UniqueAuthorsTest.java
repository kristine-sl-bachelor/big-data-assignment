package b.discover;

import b.discover.b.unique_authors.UniqueAuthorsMapper;
import b.discover.b.unique_authors.UniqueAuthorsReducer;
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

public class UniqueAuthorsTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, NullWritable > reduceDriver;

    private final String AUTHOR_1 = "Author1", AUTHOR_2 = "Author2";

    @Before
    public void setup() {

        UniqueAuthorsMapper mapper = new UniqueAuthorsMapper();
        UniqueAuthorsReducer reducer = new UniqueAuthorsReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );

    }

    @Test
    public void testMapper() throws IOException {

        String input = "<article>"
                + "<author>" + AUTHOR_1 + "</author>"
                + "<author>" + AUTHOR_2 + "</author>"
                + "</article>";

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( AUTHOR_1 ), new IntWritable( 1 ) )
                .withOutput( new Text( AUTHOR_2 ), new IntWritable( 1 ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        List<IntWritable> inputValues = new ArrayList< IntWritable >();
        inputValues.add( new IntWritable( 1 ) );

        reduceDriver.withInput( new Text( AUTHOR_1 ), inputValues )
                .withInput( new Text( AUTHOR_2 ), inputValues )
                .withOutput( new Text( AUTHOR_1 ), NullWritable.get() )
                .withOutput( new Text( AUTHOR_2 ), NullWritable.get() )
                .runTest();
    }
}
