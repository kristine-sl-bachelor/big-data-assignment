package b.discover.c.over_50;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AuthorsPublicationsReducer extends Reducer< Text, IntWritable, IntWritable, NullWritable > {

    int authors;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        authors = 0;

    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int publications = 0;

        for ( IntWritable value : values ) {

            publications += value.get();
        }

        if ( publications > AuthorsPublications.NUMBER_OF_PUBLICATIONS ) authors++;
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        context.write( new IntWritable( authors ), NullWritable.get() );
    }
}
