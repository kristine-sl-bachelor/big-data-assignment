package warmup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import warmup.unique_words.UniqueWordCountMapper;
import warmup.unique_words.UniqueWordCountReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UniqueWordsTest {

    private MapDriver< LongWritable, Text, Text, NullWritable > mapDriver;
    private ReduceDriver< Text, NullWritable, Text, NullWritable > reduceDriver;

    private final String word1 = "Word1", word2 = "Word2", word3 = "Word3", one = ", ";
    private String[] inputArray = {

            word1 + one,
            word2 + one,
            word3 + one
    };

    private String input;

    @Before
    public void setup() {

        input = "";

        for ( String i : inputArray ) {

            input += i + "\n";
        }

        UniqueWordCountMapper mapper = new UniqueWordCountMapper();
        UniqueWordCountReducer reducer = new UniqueWordCountReducer();

        mapDriver = MapDriver.newMapDriver( mapper );
        reduceDriver = ReduceDriver.newReduceDriver( reducer );

    }

    @Test
    public void testMapper() throws IOException {


        mapDriver.withInput( new LongWritable(), new Text( input ) )
                .withAllOutput( getOutput( word1, word2, word3 ) )
                .runTest();

    }

    @Test
    public void testReducer() throws IOException {

        List< NullWritable > values = new ArrayList< NullWritable >();
        values.add( NullWritable.get() );

        reduceDriver.withInput( new Text( word1 ), values )
                .withInput( new Text( word2 ), values )
                .withInput( new Text( word3 ), values )
                .withAllOutput( getOutput( word1, word2, word3 ) )
                .runTest();
    }

    private List< Pair< Text, NullWritable > > getOutput( String... text ) {

        List< Pair< Text, NullWritable > > output = new ArrayList< Pair< Text, NullWritable > >();

        for ( String t : text ) {

            output.add( new Pair< Text, NullWritable >( new Text( t ), NullWritable.get() ) );
        }

        return output;
    }
}
