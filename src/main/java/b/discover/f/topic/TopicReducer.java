package b.discover.f.topic;

import _other.helpers.MapSorter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TopicReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

    Map< String, Integer > words;
    private final File file = new File( "src/main/resources/stopwords.txt" );
    private List< String > excludedWords;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        words = new HashMap< String, Integer >();

        Scanner scanner = new Scanner( file );
        excludedWords = new ArrayList< String >();

        while ( scanner.hasNextLine() ) {

            excludedWords.add( scanner.nextLine().trim() );
        }
    }

    @Override
    protected void reduce( Text key, Iterable< IntWritable > values, Context context ) throws IOException, InterruptedException {

        for ( IntWritable value : values ) {

            String word = key.toString();

            boolean excluded = false;

            for ( String excludedWord : excludedWords ) {

                if ( word.toLowerCase().equals( excludedWord.toLowerCase() ) ) excluded = true;
            }

            if ( !excluded ) {

                if ( words.containsKey( word ) ) words.put( word, words.get( word ) + value.get() );

                else words.put( word, value.get() );

            }
        }
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {

        for ( int i = 0; i < Topic.NUMBER_OF_TOPICS; i++ ) {

            String word = MapSorter.getHighestValue( words );

            if ( word.equals( "" ) ) break;

            context.write( new Text( word ), new IntWritable( words.get( word ) ) );

            words.remove( word );
        }
    }
}
