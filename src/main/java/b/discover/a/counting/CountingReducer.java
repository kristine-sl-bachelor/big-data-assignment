package b.discover.a.counting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountingReducer extends Reducer< Text, NullWritable, IntWritable, NullWritable > {

    int topics;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        topics = 0;
    }

    @Override
    protected void reduce( Text key, Iterable< NullWritable > values, Context context ) throws IOException, InterruptedException {

        topics++;
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        context.write( new IntWritable( topics ), NullWritable.get() );
    }
}
