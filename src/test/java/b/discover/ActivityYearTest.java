package b.discover;

import b.discover.c.over_50.AuthorsPublications;
import b.discover.d.activity_year.ActivityYearMapper;
import b.discover.d.activity_year.ActivityYearReducer;
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

public class ActivityYearTest {

    private MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    private ReduceDriver< Text, IntWritable, Text, Text > reduceDriver;

    private final String AUTHOR_1 = "Author1", AUTHOR_2 = "Author2";
    private final int YEAR_1 = 2000, YEAR_2 = 2015;

    @Before
    public void setup() {

        ActivityYearMapper mapper = new ActivityYearMapper();
        ActivityYearReducer reducer = new ActivityYearReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        Text input1 = new Text( getXMLAuthorYear( AUTHOR_1, AUTHOR_2, YEAR_1 ) );
        Text input2 = new Text( getXMLAuthorYear( AUTHOR_1, AUTHOR_2, YEAR_2 ) );

        LongWritable longWritable = new LongWritable();

        mapDriver.withInput( longWritable, input1 )
                .withInput( longWritable, input2 )
                .withOutput( new Text( AUTHOR_1 ), new IntWritable( YEAR_1 ) )
                .withOutput( new Text( AUTHOR_2 ), new IntWritable( YEAR_1 ) )
                .withOutput( new Text( AUTHOR_1 ), new IntWritable( YEAR_2 ) )
                .withOutput( new Text( AUTHOR_2 ), new IntWritable( YEAR_2 ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        int[] years = { YEAR_2, YEAR_1 };

        reduceDriver.withInput( new Text( AUTHOR_1 ), getYearList( years, AuthorsPublications.NUMBER_OF_PUBLICATIONS + 1 ) )
                .withInput( new Text( AUTHOR_2 ), getYearList( years, AuthorsPublications.NUMBER_OF_PUBLICATIONS ) )
                .withOutput( new Text( AUTHOR_1 ), new Text( YEAR_1 + "-" + YEAR_2 ) )
                .runTest();
    }

    private String getXMLAuthorYear( String author1, String author2, int year ) {

        return "<article>"
                + "<author>" + author1 + "</author>"
                + "<author>" + author2 + "</author>"
                + "<year>" + year + "</year>"
                + "</article>";
    }

    private List< IntWritable > getYearList( int[] years, int publications ) {

        List< IntWritable > output = new ArrayList< IntWritable >();

        // To make sure the reducer catches it, since it needs more than a set number of publications
        for( int i = 0; i < publications; i += years.length) {

            for ( int year : years ) {

                output.add( new IntWritable( year ) );
            }
        }

        return output;
    }
}
