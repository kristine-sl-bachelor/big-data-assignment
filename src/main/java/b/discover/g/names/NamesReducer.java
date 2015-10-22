package b.discover.g.names;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NamesReducer extends Reducer< Text, IntWritable, Text, Text > {

    Map< String, Integer > firstNames, lastNames;

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
            first += i + ": " + firstTemp + "\n";

            String lastTemp = getMostPopularName( lastNames );
            lastNames.remove( lastTemp );
            last += i + ": " + lastTemp + "\n";

        }

        context.write( new Text( "Most popular first name" ), new Text( "\n" + first ) );
        context.write( new Text( "Most popular last name" ), new Text( "\n" + last ) );
    }

    private String getMostPopularName( Map< String, Integer > map ) {

        String output = "";
        int sum = 0;

        for ( Map.Entry< String, Integer > name : map.entrySet() ) {

            if ( name.getValue() > sum ) {

                output = name.getKey();
                sum = name.getValue();

            }
        }

        return output;
    }
}
