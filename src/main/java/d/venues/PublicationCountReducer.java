package d.venues;

import _other.helpers.MapSorter;
import _other.helpers.StringFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Since venues are reccurent, and several publications may belong to same venue for same year, we only count each venue
 * once per unique year. Class then outputs a list of top 10 venues, based of the abovementioned count.
 */
public class PublicationCountReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    Map< String, Integer > venues;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        venues = new HashMap< String, Integer >();
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        List< Integer > years = new ArrayList< Integer >();
        int sum = 0;

        for ( IntWritable value : values ) {

            int year = value.get();

            if ( !years.contains( year ) ) years.add( year );

            sum++;
        }

        // If it is recurrent
        if ( years.size() > 1 ) venues.put( key.toString(), sum );
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        for ( int i = 1; i <= PublicationCount.OUTPUTS; i++ ) {

            String venue = MapSorter.getHighestValue( venues );

            if ( venue.equals( "" ) ) break;

            context.write( new Text( String.format( StringFormat.POPULARITY, i, venue ) ), new IntWritable( venues.get( venue ) ) );

            venues.remove( venue );
        }
    }
}
