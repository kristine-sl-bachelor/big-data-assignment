package d.venues;

import _other.helpers.MapSorter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PublicationCountReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    Map< String, Integer > venues;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        venues = new HashMap< String, Integer >();
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        List< Integer > years = new ArrayList< Integer >();

        for ( IntWritable value : values ) {

            int year = value.get();

            if ( !years.contains( year ) ) years.add( year );
        }

        venues.put( key.toString(), years.size() );
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        for ( int i = 0; i < PublicationCount.OUTPUTS; i++ ) {

            String venue = MapSorter.getHighestValue( venues );

            context.write( new Text( venue ), new IntWritable( venues.get( venue ) ) );

            venues.remove( venue );
        }
    }
}
