package c.numbers.d.useless;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UselessReducer extends Reducer< Text, IntWritable, IntWritable, NullWritable > {

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int publications = 0;

        for ( IntWritable value : values ) {

            publications += value.get();
        }

        context.write( new IntWritable( publications ), NullWritable.get() );
    }

}
