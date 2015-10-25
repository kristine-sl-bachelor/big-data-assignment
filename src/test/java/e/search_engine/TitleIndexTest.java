package e.search_engine;

import _other.helpers.StringFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TitleIndexTest {

    MapDriver< LongWritable, Text, Text, Text > mapDriver;
    ReduceDriver< Text, Text, Text, Text > reduceDriver;
    private final String KEY = "test/testKey";

    @Before
    public void setup() {

        TitleIndexMapper mapper = new TitleIndexMapper();
        TitleIndexReducer reducer = new TitleIndexReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        String word1 = "Test", word2 = "Title";

        String input = "<article key=\"" + KEY + "\">"
                + "<title>" + word1 + " " + word2 + "</title>"
                + "</article>";

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( word1.toLowerCase() ), new Text( KEY ) )
                .withOutput( new Text( word2.toLowerCase() ), new Text( KEY ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        String inList = "a", notInList = "test";

        List< Text > values = new ArrayList< Text >();
        values.add( new Text( KEY ) );

        reduceDriver.withInput( new Text( inList ), values )
                .withInput( new Text( notInList ), values )
                .withOutput( new Text( String.format( StringFormat.WORD, notInList ) ), new Text( KEY ) )
                .runTest();
    }
}
