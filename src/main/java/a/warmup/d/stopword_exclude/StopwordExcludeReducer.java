package a.warmup.d.stopword_exclude;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Initializes a list of excluded words from a given file, and only outputs words that are not found in that list
 */
public class StopwordExcludeReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    private MultipleOutputs< Text, IntWritable > output;
    private final File file = new File( "src/main/resources/stopwords.txt" );
    private List< String > excludedWords;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        output = new MultipleOutputs< Text, IntWritable >( context );

        Scanner scanner = new Scanner( file );
        excludedWords = new ArrayList< String >();

        while ( scanner.hasNextLine() ) {

            excludedWords.add( scanner.nextLine().trim() );
        }
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        int sum = 0;

        for ( IntWritable value : values ) {

            sum += value.get();
        }

        for ( String word : excludedWords ) {

            if ( !key.toString().toLowerCase().equals( word.toLowerCase() ) ) {

                output.write( StopwordExclude.FILE_NAME, key, sum );
            }
        }
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        output.close();
    }
}
