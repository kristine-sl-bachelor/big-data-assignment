package c.numbers;

import _other.helpers.StringFormat;
import c.numbers.b.trigram.TrigramMapper;
import c.numbers.b.trigram.TrigramReducer;
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

public class TrigramTest {

    MapDriver< LongWritable, Text, Text, IntWritable > mapDriver;
    ReduceDriver< Text, IntWritable, Text, IntWritable > reduceDriver;

    private final String WORD_1 = "Word1", WORD_2 = "Word2", WORD_3 = "Word3", WORD_4 = "Word4";

    @Before
    public void setup() {

        TrigramMapper mapper = new TrigramMapper();
        TrigramReducer reducer = new TrigramReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );
    }

    @Test
    public void testMapper() throws IOException {

        String input = "<article>"
                + "<title>" + WORD_1 + " " + WORD_2 + " " + WORD_3 + " " + WORD_4 + "</title>"
                + "</article>";


        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withOutput( new Text( ( WORD_1 + " " + WORD_2 + " " + WORD_3 ).toLowerCase() ), new IntWritable( 1 ) )
                .withOutput( new Text( ( WORD_2 + " " + WORD_3 + " " + WORD_4 ).toLowerCase() ), new IntWritable( 1 ) )
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {

        List< IntWritable > values1 = new ArrayList< IntWritable >();
        List< IntWritable > values2 = new ArrayList< IntWritable >();

        values1.add( new IntWritable( 1 ) );
        values1.add( new IntWritable( 1 ) );

        values2.add( new IntWritable( 1 ) );

        String input1 = WORD_1 + " " + WORD_2 + " " + WORD_3, input2 = WORD_2 + " " + WORD_3 + " " + WORD_4;

        reduceDriver.withInput( new Text( input1 ), values1 )
                .withInput( new Text( input2 ), values2 )
                .withOutput( new Text( String.format( StringFormat.POPULARITY, 1, input1 ) ), new IntWritable( 2 ) )
                .withOutput( new Text( String.format( StringFormat.POPULARITY, 2, input2 )  ), new IntWritable( 1 ) )
                .runTest();
    }
}
