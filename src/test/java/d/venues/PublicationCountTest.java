package d.venues;

import _other.helpers.StringFormat;
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

public class PublicationCountTest {

    private final String venue = "Westerdals";
    private final int year = 2015;

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, IntWritable > reduceDriver;

    @Before
    public void setup() {

        PublicationCountMapper mapper = new PublicationCountMapper();
        PublicationCountReducer reducer = new PublicationCountReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        String input = "<article>"
                + "<tag>123</tag>"
                + "<venue>" + venue + "</venue>"
                + "<otherVenue>" + venue + "</otherVenue>"
                + "<year>" + year + "</year>"
                + "</article>";

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( venue ), new IntWritable( year ) )
                .withOutput( new Text( venue ), new IntWritable( year ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        List< IntWritable > notRecurrent = new ArrayList< IntWritable >();

        notRecurrent.add( new IntWritable( year ) );
        notRecurrent.add( new IntWritable( year ) );

        List< IntWritable > recurrent = new ArrayList< IntWritable >();

        recurrent.add( new IntWritable( year ) );
        recurrent.add( new IntWritable( year ) );
        recurrent.add( new IntWritable( year + 1 ) );

        reduceDriver.withInput( new Text( venue ), notRecurrent )
                .withInput( new Text( venue ), recurrent )
                .withOutput( new Text( String.format( StringFormat.POPULARITY, 1, venue ) ), new IntWritable( 3 ) )
                .runTest();
    }
}
