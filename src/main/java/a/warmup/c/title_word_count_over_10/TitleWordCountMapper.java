package a.warmup.c.title_word_count_over_10;

import _other.xml.XmlStringParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Outputs the title, and number of words
 */
public class TitleWordCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        List< String > titles = new XmlStringParser( value.toString() ).getValuesFromTag( "title" );

        if ( !titles.isEmpty() ) {

            StringTokenizer tokenizer = new StringTokenizer( titles.get( 0 ) );

            int words = 0;

            while ( tokenizer.hasMoreTokens() ) {

                words++;
                tokenizer.nextToken();

            }

            context.write( new Text( titles.get( 0 ) ), new IntWritable( words ) );
        }
    }
}
