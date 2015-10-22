package b.discover.g.names;

import _other.helpers.StringFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NamesReducer extends Reducer< Text, IntWritable, Text, Text > {

    Map< String, Integer > firstNames, lastNames;

    public static final Text FIRST_NAME_HEADER = new Text( "Most popular first name" ), LAST_NAME_HEADER = new Text( "Most popular last name");

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        firstNames = new HashMap< String, Integer >();
        lastNames = new HashMap< String, Integer >();
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        String name = key.toString();

        for ( IntWritable value : values ) {

            if ( value.equals( Names.FIRST_NAME ) ) {

                if ( firstNames.containsKey( name ) ) firstNames.put( name, firstNames.get( name ) + 1 );

                else firstNames.put( name, 1 );


            } else if ( value.equals( Names.LAST_NAME ) ) {

                if ( lastNames.containsKey( name ) ) lastNames.put( name, lastNames.get( name ) + 1 );

                else lastNames.put( name, 1 );
            }
        }
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        String first = "", last = "";

        for ( int i = 1; i <= 5; i++ ) {

            String firstTemp = getMostPopularName( firstNames );
            firstNames.remove( firstTemp );
            first += String.format( StringFormat.NAME_POPULARITY, i, firstTemp );

            String lastTemp = getMostPopularName( lastNames );
            lastNames.remove( lastTemp );
            last += String.format( StringFormat.NAME_POPULARITY, i, lastTemp );

        }

        context.write( FIRST_NAME_HEADER, new Text( "\n" + first ) );
        context.write( LAST_NAME_HEADER, new Text( "\n" + last ) );
    }

    private String getMostPopularName( Map< String, Integer > map ) {

        String output = "";
        int max = 0;

        for ( Map.Entry< String, Integer > name : map.entrySet() ) {

            if ( name.getValue() > max ) {

                output = name.getKey();
                max = name.getValue();

            }
        }

        return output;
    }
}
