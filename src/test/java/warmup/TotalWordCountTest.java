package warmup;

import helpers.StringFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import warmup.total_word_count.TotalWordCountMapper;
import warmup.total_word_count.TotalWordCountReducer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TotalWordCountTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, IntWritable > reduceDriver;

    private final File inputFile = new File( "src/test/resources/test_data_article_minimized.xml" );
    private String input;

    @Before
    public void setUp() throws FileNotFoundException {

        Scanner scanner = new Scanner( inputFile );
        input = scanner.nextLine();

        TotalWordCountMapper mapper = new TotalWordCountMapper();
        TotalWordCountReducer reducer = new TotalWordCountReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMap() throws IOException {

        String title1 = "Test", title2 = "Title";

        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( title1 ), new IntWritable( 1 ) )
                .withOutput( new Text( title2 ), new IntWritable( 1 ) )
                .runTest();
    }

    @Test
    public void testReduce() throws IOException {

        Text text = new Text( "Title" );

        IntWritable one = new IntWritable( 1 );
        IntWritable sum = new IntWritable( 10 );

        List< IntWritable > values = new ArrayList< IntWritable >();

        for ( int i = 0; i < sum.get(); i++ ) {

            values.add( one );
        }

        Text output = new Text( StringFormat.format( text.toString(), StringFormat.FORMAT_WORD ) );

        reduceDriver.withInput( text, values )
                .withOutput( output, sum )
                .runTest();
    }
}
