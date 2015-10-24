package _other.helpers;

import java.util.Map;

/**
 * A helper class for sorting a {@link Map} with String as key, and Integer as value.
 */
public class MapSorter {

    /**
     * Finds the key for the highest value
     *
     * @param map The given map til look through
     *
     * @return the key for the highest value in the map
     */
    public static String getHighestValue( Map< String, Integer > map ) {

        int max = 0;
        String output = "";

        for ( Map.Entry< String, Integer > entry : map.entrySet() ) {

            if ( entry.getValue() > max ) {

                max = entry.getValue();
                output = entry.getKey();
            }
        }

        return output;
    }
}
