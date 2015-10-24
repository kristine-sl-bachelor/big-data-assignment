package c.numbers.b.trigram;

import _other.helpers.MapSorter;
import _other.helpers.StringFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Outputs the most used trigrams
 */
public class TrigramReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    Map< String, Integer > trigrams;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        trigrams = new HashMap< String, Integer >();
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int sum = 0;

        for ( IntWritable value : values ) {

            sum += value.get();
        }

        trigrams.put( key.toString(), sum );
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        for ( int i = 1; i <= Trigram.OUTPUTS; i++ ) {

            String trigram = MapSorter.getHighestValue( trigrams );
            context.write( new Text( String.format( StringFormat.POPULARITY, i, trigram ) ), new IntWritable( trigrams.get( trigram ) ) );

            trigrams.remove( trigram );
        }
    }
}
