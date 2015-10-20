package warmup.stopword_exclude;

import helpers.Word;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class StopwordExcludeMapper extends Mapper< LongWritable, Text, Text, NullWritable > {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer( value.toString() );

        while ( tokenizer.hasMoreTokens() ) {

            context.write( new Text( Word.cleanWord( tokenizer.nextToken() ) ), NullWritable.get() );
        }
    }
}
