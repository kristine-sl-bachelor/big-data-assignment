package warmup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import warmup.title_word_count_over_10.TitleWordCountMapper;
import warmup.title_word_count_over_10.TitleWordCountReducer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TitleWordCountTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, IntWritable, NullWritable > reduceDriver;

    private final File inputFile = new File( "src/test/resources/test_data_article_minimized.xml" );
    private String input;

    @Before
    public void setup() throws FileNotFoundException {

        Scanner scanner = new Scanner( inputFile );
        input = scanner.nextLine();

        TitleWordCountMapper mapper = new TitleWordCountMapper();
        TitleWordCountReducer reducer = new TitleWordCountReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        mapDriver.withInput( new LongWritable(  ), new Text( input ))
                .withOutput( new Text( "Test Title" ), new IntWritable( 2 ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        List<IntWritable> values1 = new ArrayList<IntWritable>();
        List<IntWritable> values2 = new ArrayList<IntWritable>();
        List<IntWritable> values3 = new ArrayList<IntWritable>();

        values1.add( new IntWritable( 1 ) );
        values2.add( new IntWritable( 10 ) );
        values3.add( new IntWritable( 20 ) );

        // Number of words should already be counted in mapper, no need to use titles with corresponding number of words
        Text title1 = new Text( "Title1" );
        Text title2 = new Text( "Title2" );
        Text title3 = new Text( "Title3" );

        reduceDriver.withInput( title1,  values1)
                .withInput( title2, values2)
                .withInput( title3, values3 )
                .withOutput( new IntWritable( 2 ), NullWritable.get() )
                .runTest();
    }
}
