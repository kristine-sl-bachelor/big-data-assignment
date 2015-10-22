package _other.helpers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.ArrayList;
import java.util.List;

public class DriverOutput {

    public static List< Pair< Text, NullWritable > > getOutputTextNull( String... text ) {

        List< Pair< Text, NullWritable > > output = new ArrayList< Pair< Text, NullWritable > >();

        for ( String t : text ) {

            output.add( new Pair< Text, NullWritable >( new Text( t ), NullWritable.get() ) );
        }

        return output;
    }

    public static List< Pair< Text, IntWritable > > getOutputTextOne( String... text ) {

        List< Pair< Text, IntWritable > > output = new ArrayList< Pair< Text, IntWritable > >();

        for ( String t : text ) {

            output.add( new Pair< Text, IntWritable >( new Text( t ), new IntWritable( 1 ) ) );
        }

        return output;
    }
}
