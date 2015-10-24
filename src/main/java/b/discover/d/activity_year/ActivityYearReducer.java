package b.discover.d.activity_year;

import b.discover.c.over_50.AuthorsPublications;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Finds the oldest and newest registerred years, and outputs the author and years.
 */
public class ActivityYearReducer extends Reducer< Text, IntWritable, Text, Text > {

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int publications = 0;
        int minYear = Integer.MAX_VALUE, maxYear = Integer.MIN_VALUE; // To be 100% certain it will be changed first time

        for ( IntWritable value : values ) {

            if ( value.get() < minYear ) minYear = value.get();

            if ( value.get() > maxYear ) maxYear = value.get();

            publications++;
        }

        if ( publications > AuthorsPublications.NUMBER_OF_PUBLICATIONS ) context.write( key, new Text( minYear + "-" + maxYear ) );
    }
}
