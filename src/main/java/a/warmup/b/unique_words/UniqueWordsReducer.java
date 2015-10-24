package a.warmup.b.unique_words;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Finds the words that have only been used once, and prints the word (no counter)
 */
public class UniqueWordsReducer extends Reducer< Text, IntWritable, Text, NullWritable > {

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int sum = 0;

        for( IntWritable value : values ) {

            sum += value.get();
        }

        if( sum == 1) context.write( key, NullWritable.get() );
    }
}