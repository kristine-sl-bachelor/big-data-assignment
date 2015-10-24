package _other.helpers;

import java.util.Map;

public class MapSorter {

    public static String getHighestValue( Map< String, Integer > map ) {

        int max = 0;
        String output = "";

        for ( Map.Entry<String, Integer> entry : map.entrySet() ) {

            if ( entry.getValue() > max ) {

                max = entry.getValue();
                output = entry.getKey();
            }
        }

        return output;
    }
}
