package c.numbers.a.bigram;

import _other.helpers.MapSorter;
import _other.helpers.StringFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BigramReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    Map< String, Integer > bigrams;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        bigrams = new HashMap< String, Integer >();
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int sum = 0;

        for ( IntWritable value : values ) {

            sum += value.get();
        }

        bigrams.put( key.toString(), sum );

    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        for( int i = 1; i <= Bigram.OUTPUTS; i++ ) {

            String bigram = MapSorter.getHighestValue( bigrams );
            context.write( new Text( String.format( StringFormat.POPULARITY, i, bigram ) ), new IntWritable( bigrams.get( bigram ) )  );

            bigrams.remove( bigram );
        }
    }
}
