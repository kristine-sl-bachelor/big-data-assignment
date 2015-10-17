package warmup.unique_words;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UniqueWordsReducer extends Reducer< Text, NullWritable, Text, NullWritable > {

    /**
     * Finds the words that have only been used once, and prints the word, without the sum (the sum is always 1)
     */
    @Override
    protected void reduce( Text key, Iterable< NullWritable > values, Context context ) throws IOException, InterruptedException {

        context.write( key, NullWritable.get() );
    }
}