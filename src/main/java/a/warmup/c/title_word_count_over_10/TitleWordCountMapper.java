package a.warmup.c.title_word_count_over_10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import _other.xml.XmlStringParser;

import java.io.IOException;
import java.util.StringTokenizer;


public class TitleWordCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final Text WORD = new Text();

    /**
     *
     */
    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        String title = new XmlStringParser( value.toString() ).getValueFromTag( "title" );

        StringTokenizer tokenizer = new StringTokenizer( title );

        int words = 0;

        while ( tokenizer.hasMoreTokens() ) {

            words++;
            tokenizer.nextToken();

        }

        context.write( new Text( title ), new IntWritable( words ) );
    }
}
