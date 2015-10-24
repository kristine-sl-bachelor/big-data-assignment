package b.discover;

import b.discover.c.over_50.AuthorsPublications;
import b.discover.c.over_50.AuthorsPublicationsMapper;
import b.discover.c.over_50.AuthorsPublicationsReducer;
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

public class AuthorsPublicationsTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, IntWritable, NullWritable > reduceDriver;

    private String AUTHOR_1 = "Author 1", AUTHOR_2 = "Author 2";

    @Before
    public void setup() {

        AuthorsPublicationsMapper mapper = new AuthorsPublicationsMapper();
        AuthorsPublicationsReducer reducer = new AuthorsPublicationsReducer();

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

        reduceDriver.withInput( new Text( AUTHOR_1 ), getIntList( 1, AuthorsPublications.NUMBER_OF_PUBLICATIONS + 1) )
                .withInput( new Text( AUTHOR_2 ), getIntList( 1, AuthorsPublications.NUMBER_OF_PUBLICATIONS ) )
                .withOutput( new IntWritable( 1 ), NullWritable.get() )
                .runTest();
    }

    private List< IntWritable > getIntList( int value, int publications ) {

        List< IntWritable > output = new ArrayList< IntWritable >();

        for ( int i = 0; i < publications; i++ ) {

            output.add( new IntWritable( value ) );
        }

        return output;
    }
}
