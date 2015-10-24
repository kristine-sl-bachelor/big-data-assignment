package b.discover;

import _other.helpers.StringFormat;
import b.discover.c.over_50.AuthorsPublications;
import b.discover.e.diversity.DiversityMapper;
import b.discover.e.diversity.DiversityReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DiversityTest {

    MapDriver< LongWritable, Text, Text, Text > mapDriver;
    ReduceDriver< Text, Text, Text, Text > reduceDriver;

    private final String AUTHOR = "Author";
    private final String TYPE_1 = "typeone", TYPE_2 = "typetwo";

    @Before
    public void setup() {

        DiversityMapper mapper = new DiversityMapper();
        DiversityReducer reducer = new DiversityReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );

    }

    @Test
    public void testMapper() throws IOException {

        mapDriver.withInput( new LongWritable(), new Text( getInput( TYPE_1 ) ) )
                .withInput( new LongWritable(), new Text( getInput( TYPE_2 ) ) )
                .withOutput( new Text( AUTHOR ), new Text( TYPE_1 ) )
                .withOutput( new Text( AUTHOR ), new Text( TYPE_2 ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        int numberOfType = AuthorsPublications.NUMBER_OF_PUBLICATIONS + 1;

        List< Text > type1 = getInputList( TYPE_1, numberOfType );

        reduceDriver.withInput( new Text( AUTHOR ), type1 )
                .withOutput( new Text( AUTHOR ), new Text( String.format( StringFormat.TYPE, TYPE_1, numberOfType ) ) )
                .runTest();
    }

    private String getInput( String type ) {

        return "<" + type + ">"
                + "<author>" + AUTHOR + "</author>"
                + "</" + type + ">";

    }

    private List< Text > getInputList( String input, int publications ) {

        List< Text > output = new ArrayList< Text >();

        for ( int i = 0; i < publications; i++ ) {

            output.add( new Text( input ) );
        }

        return output;
    }
}
