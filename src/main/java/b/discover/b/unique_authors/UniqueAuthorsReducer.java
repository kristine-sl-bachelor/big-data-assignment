package b.discover.b.unique_authors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UniqueAuthorsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        context.write( key, NullWritable.get() );
    }
}
