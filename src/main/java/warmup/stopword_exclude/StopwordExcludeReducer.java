package warmup.stopword_exclude;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class StopwordExcludeReducer extends Reducer< Text, NullWritable, Text, NullWritable > {

    private MultipleOutputs< Text, NullWritable > output;
    private final File file = new File( "src/main/resources/stopwords.txt" );
    private List< String > excludedWords;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        super.setup( context );

        output = new MultipleOutputs< Text, NullWritable >( context );

        Scanner scanner = new Scanner( file );
        excludedWords = new ArrayList< String >();

        while ( scanner.hasNextLine() ) {

            excludedWords.add( scanner.nextLine().trim() );
        }
    }

    @Override
    protected void reduce( Text key, Iterable< NullWritable > values, Context context ) throws IOException, InterruptedException {

        for ( String word : excludedWords ) {

            if ( key.toString().toLowerCase().equals( word.toLowerCase() ) )
                output.write( StopwordExclude.FILE_NAME, key, NullWritable.get() );
        }
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        output.close();
    }
}
