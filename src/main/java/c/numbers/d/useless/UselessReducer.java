package c.numbers.d.useless;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UselessReducer extends Reducer< Text, IntWritable, IntWritable, NullWritable > {

    private int publications;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        publications = 0;
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        for ( IntWritable value : values ) {

            publications += value.get();
        }
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        context.write( new IntWritable( publications ), NullWritable.get() );
    }
}
