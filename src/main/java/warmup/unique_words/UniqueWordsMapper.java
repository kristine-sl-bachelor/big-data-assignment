package warmup.unique_words;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Scanner;


public class UniqueWordsMapper extends Mapper< LongWritable, Text, Text, NullWritable > {

    private final Text WORD = new Text();

    /**
     * Goes through data token by token, verifies that token is a word, and removes any characters that are not letters
     * from the word.
     */
    @Override

    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        Scanner scanner = new Scanner( value.toString() );

        while ( scanner.hasNextLine() ) {

            String[] tokens = scanner.nextLine().split( ", " );

            System.out.println( tokens );

            context.write( new Text( tokens[ 0 ] ), NullWritable.get() );
        }
    }
}