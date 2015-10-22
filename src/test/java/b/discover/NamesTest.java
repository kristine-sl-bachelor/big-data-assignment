package b.discover;

import _other.helpers.StringFormat;
import b.discover.g.names.Names;
import b.discover.g.names.NamesMapper;
import b.discover.g.names.NamesReducer;
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

public class NamesTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, Text > reduceDriver;

    private List< String > firstNames, lastNames;
    private final String FIRST_NAME = "First", LAST_NAME = "Last";

    @Before
    public void setup() {

        NamesMapper mapper = new NamesMapper();
        NamesReducer reducer = new NamesReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );

        firstNames = new ArrayList< String >();
        lastNames = new ArrayList< String >();

        for ( int i = 0; i < 6; i++ ) {

            firstNames.add( FIRST_NAME + i );
            lastNames.add( LAST_NAME + i );
        }

    }

    @Test
    public void testMapper() throws IOException {

        Text input = new Text( "<article>"
                + "<author>" + FIRST_NAME + " " + LAST_NAME + " " + LAST_NAME + "</author>"
                + "</article>" );

        mapDriver.withInput( new LongWritable(), input )
                .withOutput( new Text( FIRST_NAME ), Names.FIRST_NAME )
                .withOutput( new Text( LAST_NAME ), Names.LAST_NAME )
                .withOutput( new Text( LAST_NAME ), Names.LAST_NAME )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        int notPopular = 1;
        int nrOne = 6, nrTwo = 5, nrThree = 4, nrFour = 3, nrFive = 2;

        String firstNameOutput = "", lastNameOutput = "";

        for ( int i = 1; i <= 5; i++ ) {

            firstNameOutput += String.format( StringFormat.NAME_POPULARITY, i, firstNames.get( i - 1 ) );
            lastNameOutput += String.format( StringFormat.NAME_POPULARITY, i, lastNames.get( i - 1 ) );
        }

        reduceDriver.withInput( new Text( firstNames.get( 0 ) ), getInputList( Names.FIRST_NAME, nrOne ) )
                .withInput( new Text( firstNames.get( 1 ) ), getInputList( Names.FIRST_NAME, nrTwo ) )
                .withInput( new Text( firstNames.get( 2 ) ), getInputList( Names.FIRST_NAME, nrThree ) )
                .withInput( new Text( firstNames.get( 3 ) ), getInputList( Names.FIRST_NAME, nrFour ) )
                .withInput( new Text( firstNames.get( 4 ) ), getInputList( Names.FIRST_NAME, nrFive ) )

                .withInput( new Text( firstNames.get( 5 ) ), getInputList( Names.FIRST_NAME, notPopular ) )

                .withInput( new Text( lastNames.get( 0 ) ), getInputList( Names.LAST_NAME, nrOne ) )
                .withInput( new Text( lastNames.get( 1 ) ), getInputList( Names.LAST_NAME, nrTwo ) )
                .withInput( new Text( lastNames.get( 2 ) ), getInputList( Names.LAST_NAME, nrThree ) )
                .withInput( new Text( lastNames.get( 3 ) ), getInputList( Names.LAST_NAME, nrFour ) )
                .withInput( new Text( lastNames.get( 4 ) ), getInputList( Names.LAST_NAME, nrFive ) )

                .withInput( new Text( lastNames.get( 5 ) ), getInputList( Names.LAST_NAME, notPopular ) )

                .withOutput( NamesReducer.FIRST_NAME_HEADER, new Text( "\n" + firstNameOutput ) )
                .withOutput( NamesReducer.LAST_NAME_HEADER, new Text( "\n" + lastNameOutput ) )

                .runTest();
    }

    private List< IntWritable > getInputList( IntWritable nameType, int iterations ) {

        List< IntWritable > output = new ArrayList< IntWritable >();

        for ( int i = 0; i < iterations; i++ ) {

            output.add( nameType );
        }

        return output;
    }
}
