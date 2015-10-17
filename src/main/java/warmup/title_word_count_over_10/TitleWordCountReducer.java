package warmup.title_word_count_over_10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TitleWordCountReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    /**
     *
     */
    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {


    }
}

