package b.discover.e.diversity;

import _other.helpers.StringFormat;
import b.discover.c.over_50.AuthorsPublications;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DiversityReducer extends Reducer< Text, Text, Text, Text > {

    @Override
    protected void reduce( Text key, Iterable< Text > values, Context context ) throws IOException, InterruptedException {

        HashMap< String, Integer > types = new HashMap< String, Integer >();

        int publications = 0;

        for ( Text value : values ) {

            String type = value.toString();

            if ( types.containsKey( type ) ) types.put( type, types.get( type ) + 1 );

            else types.put( type, 1 );

            publications++;

        }

        if ( publications > AuthorsPublications.NUMBER_OF_PUBLICATIONS ) {

            String allTypes = "";

            int i = 0;

            for ( Map.Entry< String, Integer > entry : types.entrySet() ) {

                if ( i != 0 ) allTypes += ", ";

                allTypes += String.format( StringFormat.TYPE, entry.getKey(), entry.getValue() );

                i++;
            }

            context.write( key, new Text( allTypes ) );
        }

    }
}
