package b.discover.a.counting;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Outputs the name of the current root tag, which is the identifyer for the publication
 */
public class CountingMapper extends Mapper< LongWritable, Text, Text, NullWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        String type = new XmlStringParser( value.toString() ).getRootTagName();

        context.write( new Text( type ), NullWritable.get() );
    }
}
