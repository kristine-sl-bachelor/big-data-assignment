package c.numbers.e.method;

import _other.helpers.Word;
import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class MethodMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        List< String > titles = new XmlStringParser( value.toString() ).getValuesFromTag( "title" );

        if ( !titles.isEmpty() ) {

            StringTokenizer tokenizer = new StringTokenizer( titles.get( 0 ) );

            while ( tokenizer.hasMoreTokens() ) {

                if ( Word.cleanWord( tokenizer.nextToken().toLowerCase() ).equals( Method.WORD ) ) {

                    context.write( new Text( Method.WORD ), new IntWritable( 1 ) );
                }
            }
        }
    }
}
