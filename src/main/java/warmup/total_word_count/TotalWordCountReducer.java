package warmup.total_word_count;

import helpers.StringFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TotalWordCountReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    /**
     * Finds the sum for the number of times the word is used, and prints the word and the sum.
     * Formatted with 50 spaces for word.
     */
    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int sum = 0;

        for ( IntWritable value : values ) {

            sum += value.get();
        }

        Text output = new Text( StringFormat.format( key.toString(), StringFormat.FORMAT_WORD ) );

        context.write( output, new IntWritable( sum ) );
    }
}