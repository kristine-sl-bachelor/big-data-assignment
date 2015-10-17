package warmup.title_word_count_over_10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Scanner;


public class TitleWordCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final Text WORD = new Text();

    /**
     *
     */
    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        Scanner scanner = new Scanner( value.toString() );

        while( scanner.hasNextLine() ) {

            String word = scanner.nextLine();
            context.write( new Text(  ), new IntWritable( scanner.nextLine().length() ) );
        }
    }
}
